import boto3
import contextlib
import eventlet
import gzip
import os
import pandas as pd
import ujson as json
import urllib

from bisect import bisect
from boto3.s3.transfer import S3Transfer
from datetime import datetime, timedelta
from eventlet.green import urllib2
from moztelemetry import get_pings_properties
from moztelemetry.dataset import Dataset
from sets import Set
from StringIO import StringIO

from profile import ProfileProcessor

UNSYMBOLICATED = "<unsymbolicated>"

def get_data(sc, config, start_date_relative, end_date_relative):
    start_date = (datetime.today() + timedelta(days=start_date_relative))
    start_date_str = start_date.strftime("%Y%m%d")
    end_date = (datetime.today() + timedelta(days=end_date_relative))
    end_date_str = end_date.strftime("%Y%m%d")

    pings = (Dataset.from_source("telemetry")
        .where(docType='main')
        .where(appBuildId=lambda b: (b.startswith(start_date_str) or b > start_date_str)
                                     and (b.startswith(end_date_str) or b < end_date_str))
        .where(appUpdateChannel="nightly")
        .records(sc, sample=config['sample_size']))

    properties = ["environment/system/os/name",
                  "application/buildId",
                  "payload/info/subsessionLength",
                  "payload/childPayloads",
                  "payload/threadHangStats"]

    return get_pings_properties(pings, properties, with_processes=True)

def windows_only(p):
    return p["environment/system/os/name"] == "Windows_NT"

def ping_is_valid(ping):
    if not isinstance(ping["application/buildId"], basestring):
        return False
    if type(ping["payload/info/subsessionLength"]) != int:
        return False

    return True

def flatten_hangs(build_date, thread_hang):
    if 'name' not in thread_hang:
        return []

    hangs = thread_hang['hangs']
    if 'nativeStacks' in thread_hang:
        hangs = [
            {
                'stack': x['stack'],
                'nativeStack': {
                    'memoryMap': thread_hang['nativeStacks']['memoryMap'],
                    'stacks': [thread_hang['nativeStacks']['stacks'][x['nativeStack']]],
                },
                'histogram': x['histogram'],
            }
            for x in hangs
            if 'nativeStack' in x
        ]

    return [
        {
            'build_date': build_date,
            'thread_name': thread_hang['name'],
            'hang': x
        }
        for x in hangs
        if 'nativeStack' in x
        and type(x['nativeStack']) is dict
        and len(x['nativeStack']['stacks']) > 0
        and len(x['nativeStack']['stacks'][0]) > 0
    ]

def only_hangs_of_type(ping):
    result = []

    build_date = ping["application/buildId"][:8] # "YYYYMMDD" : 8 characters
    usage_hours = float(ping['payload/info/subsessionLength']) / 60.0

    if usage_hours == 0:
        return result

    if ping['payload/childPayloads'] is not None:
        for payload in ping['payload/childPayloads']:
            if 'threadHangStats' not in payload:
                continue

            for thread_hang in payload['threadHangStats']:
                result = result + flatten_hangs(build_date, thread_hang)

    if ping['payload/threadHangStats'] is not None:
        for thread_hang in ping['payload/threadHangStats']:
            result = result + flatten_hangs(build_date, thread_hang)

    return result

def filter_for_hangs_of_type(pings):
    return pings.flatMap(lambda p: only_hangs_of_type(p))

def map_to_frame_info(hang):
    memory_map = hang['hang']['nativeStack']['memoryMap']
    stack = hang['hang']['nativeStack']['stacks'][0]
    return [
        (tuple(memory_map[module_index]), (offset,)) if module_index != -1 else (None, (offset,))
        for module_index, offset in stack
    ]

def get_stacks_by_module(hangs):
    return (hangs.flatMap(map_to_frame_info)
        .filter(lambda hang_tuple: hang_tuple[0] is not None)
        .distinct()
        .reduceByKey(lambda a,b: a + b).collectAsMap())

def symbolicate_stacks(memory_map, stack, processed_modules):
    symbolicated = []
    num_symbolicated = 0
    for module_index, offset in stack:
        if module_index != -1:
            debug_name, breakpad_id = memory_map[module_index]
            processed = processed_modules[breakpad_id, offset]
            if processed is not None:
                symbolicated.append(processed)
                num_symbolicated += 1
            else:
                symbolicated.append(format_frame(UNSYMBOLICATED, debug_name))
        else:
            symbolicated.append(format_frame(UNSYMBOLICATED, "unknown"))
    return symbolicated, float(num_symbolicated) / float(len(symbolicated))

def get_symbolicated_stack(hang, processed_modules, usage_hours_by_date):
    build_date = hang['build_date']
    usage_hours = usage_hours_by_date[build_date]

    if usage_hours <= 0:
        return None

    memory_map = hang['hang']['nativeStack']['memoryMap']
    native_stack = hang['hang']['nativeStack']['stacks'][0]
    symbolicated, percent_symbolicated = symbolicate_stacks(memory_map, native_stack, processed_modules)

    # We only want mostly-symbolicated stacks. Anything else is A) not useful
    # information, and B) probably a local build, which could be hanging for
    # much different reasons.
    if percent_symbolicated < 0.8:
        return None

    return tuple(symbolicated)

def get_pseudo_stack(hang, usage_hours_by_date):
    build_date = hang['build_date']
    usage_hours = usage_hours_by_date[build_date]

    if usage_hours <= 0:
        return None
    return tuple(hang['hang']['stack'])

def map_to_hang_data(hang, processed_modules, usage_hours_by_date, symbolicated_stacks_to_ids, pseudo_stacks_to_ids):
    hist_data = hang['hang']['histogram']['values']
    key_ints = map(int, hist_data.keys())
    hist = pd.Series(hist_data.values(), index=key_ints)
    weights = pd.Series(key_ints, index=key_ints)
    hang_sum = (hist * weights)[hist.index >= 100].sum()
    hang_count = hist[hist.index >= 100].sum()
    build_date = hang['build_date']
    usage_hours = usage_hours_by_date[build_date]

    if usage_hours <= 0:
        return None

    memory_map = hang['hang']['nativeStack']['memoryMap']
    native_stack = hang['hang']['nativeStack']['stacks'][0]
    symbolicated, percent_symbolicated = symbolicate_stacks(memory_map, native_stack, processed_modules)

    # We only want mostly-symbolicated stacks. Anything else is A) not useful
    # information, and B) probably a local build, which could be hanging for
    # much different reasons.
    if percent_symbolicated < 0.8:
        return None

    key = (
        symbolicated_stacks_to_ids[tuple(symbolicated)],
        pseudo_stacks_to_ids[tuple(hang['hang']['stack'])],
        hang['thread_name'],
        build_date)
    return (key, {
        'hang_ms': float(hang_sum) / usage_hours,
        'hang_count': float(hang_count) / usage_hours,
    })

def get_all_symbolicated_stacks(hangs, processed_modules, usage_hours_by_date):
    return (hangs.map(lambda hang: get_symbolicated_stack(hang, processed_modules, usage_hours_by_date))
        .filter(lambda hang: hang is not None)
        .distinct()
        .collect())

def get_all_pseudo_stacks(hangs, usage_hours_by_date):
    return (hangs.map(lambda hang: get_pseudo_stack(hang, usage_hours_by_date))
        .filter(lambda hang: hang is not None)
        .distinct()
        .collect())

def merge_hang_data(a, b):
    return {
        'hang_ms': a['hang_ms'] + b['hang_ms'],
        'hang_count': a['hang_count'] + b['hang_count'],
    }

def get_grouped_sums_and_counts(hangs, processed_modules, usage_hours_by_date, symbolicated_stacks_to_ids, pseudo_stacks_to_ids):
    return (hangs.map(lambda hang: map_to_hang_data(hang, processed_modules, usage_hours_by_date,
            symbolicated_stacks_to_ids, pseudo_stacks_to_ids))
        .filter(lambda hang: hang is not None)
        .reduceByKey(merge_hang_data)
        .map(lambda hang: hang[0] + (hang[1]['hang_ms'], hang[1]['hang_count']))
        .collect())

def get_usage_hours(ping):
    build_date = ping["application/buildId"][:8] # "YYYYMMDD" : 8 characters
    usage_hours = float(ping['payload/info/subsessionLength']) / 60.0
    return (build_date, usage_hours)

def merge_usage_hours(a, b):
    return a + b

def get_usage_hours_by_date(pings):
    return pings.map(get_usage_hours).reduceByKey(merge_usage_hours).collectAsMap()

def make_sym_map(data):
    public_symbols = {}
    func_symbols = {}

    for line in data.splitlines():
        if line.startswith("PUBLIC "):
            line = line.rstrip()
            fields = line.split(" ", 3)
            if len(fields) < 4:
                continue
            address = int(fields[1], 16)
            symbol = fields[3]
            public_symbols[address] = symbol
        elif line.startswith("FUNC "):
            line = line.rstrip()
            fields = line.split(" ", 4)
            if len(fields) < 5:
                continue
            address = int(fields[1], 16)
            symbol = fields[4]
            func_symbols[address] = symbol
    # Prioritize PUBLIC symbols over FUNC ones
    sym_map = func_symbols
    sym_map.update(public_symbols)

    return sorted(sym_map), sym_map

def get_file_URL(module, config):
    lib_name, breakpad_id = module
    if lib_name.endswith(".pdb"):
        file_name = lib_name[:-4] + ".sym"
    else:
        file_name = lib_name + ".sym"

    return config['symbol_server_url'] + "/".join([
        urllib.quote_plus(lib_name),
        urllib.quote_plus(breakpad_id),
        urllib.quote_plus(file_name)
    ])

def process_modules(stacks_by_module, config):
    stack_dict = {}

    file_URLs = [
        get_file_URL(module, config)
        for module, offsets in stacks_by_module.iteritems()
    ]
    pool = eventlet.GreenPool()

    print "({} distinct module URLs)".format(len(file_URLs))

    for (success, response), (module, offsets) in zip(pool.imap(fetch_URL, file_URLs), stacks_by_module.iteritems()):
        module_name, breakpad_id = module

        if success:
            sorted_keys, sym_map = make_sym_map(response)

            for offset in offsets:
                i = bisect(sorted_keys, offset)
                key = sorted_keys[i - 1] if i else None

                symbol = sym_map.get(key)
                if symbol is not None:
                    stack_dict[breakpad_id, offset] = format_frame(symbol, module_name)
                else:
                    stack_dict[breakpad_id, offset] = format_frame(UNSYMBOLICATED, module_name)
        else:
            for offset in offsets:
                stack_dict[breakpad_id, offset] = format_frame(UNSYMBOLICATED, module_name)

    return stack_dict

def transform_pings(pings, config):
    print "Filtering to Windows pings..."
    windows_pings_only = pings.filter(windows_only).filter(ping_is_valid)
    windows_pings_only.cache()

    print "Filtering to hangs with native stacks..."
    hangs = filter_for_hangs_of_type(windows_pings_only)
    hangs.cache()

    print "Getting stacks by module..."
    stacks_by_module = get_stacks_by_module(hangs)
    print "Processing modules..."
    processed_modules = process_modules(stacks_by_module, config)

    print "Getting usage hours..."
    usage_hours_by_date = get_usage_hours_by_date(windows_pings_only)

    print "Getting all symbolicated stacks..."
    symbolicated_stacks = get_all_symbolicated_stacks(hangs, processed_modules, usage_hours_by_date)
    symbolicated_stacks_to_ids = {stack: index for index, stack in enumerate(symbolicated_stacks)}

    print "Getting all pseudo stacks..."
    pseudo_stacks = get_all_pseudo_stacks(hangs, usage_hours_by_date)
    pseudo_stacks_to_ids = {stack: index for index, stack in enumerate(pseudo_stacks)}

    print "Grouping stacks..."
    result = get_grouped_sums_and_counts(hangs,
        processed_modules, usage_hours_by_date,
        symbolicated_stacks_to_ids, pseudo_stacks_to_ids)
    return {
        'grouped_sums_and_counts': result,
        'symbolicated_stacks': symbolicated_stacks,
        'pseudo_stacks': pseudo_stacks,
    }

def fetch_URL(url):
    result = False, ""
    try:
        with contextlib.closing(urllib2.urlopen(url)) as response:
            responseCode = response.getcode()
            if responseCode == 404:
                return False, ""
            if responseCode != 200:
                result = False, ""
            return True, decode_response(response)
    except IOError as e:
        result = False, ""

    if not result[0]:
        try:
            with contextlib.closing(urllib2.urlopen(url)) as response:
                responseCode = response.getcode()
                if responseCode == 404:
                    return False, ""
                if responseCode != 200:
                    result = False, ""
                return True, decode_response(response)
        except IOError as e:
            result = False, ""

    return result

def decode_response(response):
    headers = response.info()
    content_encoding = headers.get("Content-Encoding", "").lower()
    if content_encoding in ("gzip", "x-gzip", "deflate"):
        with contextlib.closing(StringIO(response.read())) as data_stream:
            try:
                with gzip.GzipFile(fileobj=data_stream) as f:
                    return f.read()
            except EnvironmentError:
                data_stream.seek(0)
                return data_stream.read().decode('zlib')
    return response.read()

def format_frame(symbol, module_name):
    return "{} (in {})".format(symbol, module_name)

def smart_hex(offset):
    return hex(int(offset))

def write_file(name, stuff, config):
    end_date = datetime.today()
    end_date_str = end_date.strftime("%Y%m%d")

    filename = "./output/%s-%s.json" % (name, end_date_str)
    gzfilename = filename + '.gz'
    jsonblob = json.dumps(stuff, ensure_ascii=False)

    if not os.path.exists('./output'):
        os.makedirs('./output')
    with open(filename, 'w') as f:
        f.write(jsonblob)
    with gzip.open(gzfilename, 'w') as f:
        f.write(jsonblob)

    if config['use_s3']:
        bucket = "telemetry-public-analysis-2"
        timestamped_s3_key = "bhr/data/hang_aggregates/" + name + ".json"
        client = boto3.client('s3', 'us-west-2')
        transfer = S3Transfer(client)
        transfer.upload_file(filename,
                             bucket,
                             timestamped_s3_key,
                             extra_args={'ContentType':'application/json'})
        transfer.upload_file(gzfilename,
                             bucket,
                             timestamped_s3_key,
                             extra_args={
                                'ContentType':'application/json',
                                'ContentEncoding':'gzip'
                            })

def etl_job(sc, sqlContext, config=None):
    """This is the function that will be executed on the cluster"""

    final_config = {
        'days_to_aggregate': 30,
        'use_s3': True,
        'sample_size': 0.1,
        'symbol_server_url': "https://s3-us-west-2.amazonaws.com/org.mozilla.crash-stats.symbols-public/v1/"
    }

    if config is not None:
        final_config.update(config)

    profile_processor = ProfileProcessor()
    # We were OOMing trying to allocate a contiguous array for all of this. Pass it in
    # bit by bit to the profile processor and hope it can handle it.
    for x in xrange(1, final_config['days_to_aggregate'] + 1):
        transformed = transform_pings(get_data(sc, final_config, -x, -x), final_config)
        print "Passing stacks to processor..."
        profile_processor.ingest(transformed)

    profile = profile_processor.process_into_profile()
    write_file('hang_profile', profile, final_config)
