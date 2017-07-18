import boto3
import contextlib
import gc
import gzip
import os
import pandas as pd
import Queue
import sys
import threading
import ujson as json
import urllib
import urllib2

from bisect import bisect
from boto3.s3.transfer import S3Transfer
from datetime import datetime, timedelta
from moztelemetry import get_pings_properties
from moztelemetry.dataset import Dataset
from sets import Set
from StringIO import StringIO

from profile import ProfileProcessor

UNSYMBOLICATED = "<unsymbolicated>"
REDUCE_BY_KEY_PARALLELISM = 512

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
            'runnable_name': thread_hang['runnableName'] if 'runnableName' in thread_hang else '---',
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
        .reduceByKey(lambda a,b: a + b, REDUCE_BY_KEY_PARALLELISM)
        .collectAsMap())

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
            symbolicated.append(format_frame(UNSYMBOLICATED, 'unknown'))
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

def map_to_hang_data(hang, config):
    hist_data = hang['hang']['histogram']['values']
    key_ints = map(int, hist_data.keys())
    hist = pd.Series(hist_data.values(), index=key_ints)
    weights = pd.Series(key_ints, index=key_ints)
    minned_sum = (hist * weights)[hist.index >= config['hang_lower_bound']]
    hang_sum = minned_sum[minned_sum.index < config['hang_upper_bound']].sum()
    minned = hist[hist.index >= config['hang_lower_bound']]
    hang_count = minned[minned.index < config['hang_upper_bound']].sum()
    build_date = hang['build_date']
    memory_map = hang['hang']['nativeStack']['memoryMap']
    native_stack = hang['hang']['nativeStack']['stacks'][0]

    key = (
        tuple((a,b) for a,b in native_stack),
        tuple((a,b) for a,b in memory_map),
        tuple(hang['hang']['stack']),
        hang['runnable_name'],
        hang['thread_name'],
        build_date)
    return (key, (
        float(hang_sum),
        float(hang_count),
    ))

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
    return (
        a[0] + b[0],
        a[1] + b[1],
    )

def process_hang_key(key, processed_modules):
    symbolicated, percent_symbolicated = symbolicate_stacks(key[1], key[0], processed_modules)

    # We only want mostly-symbolicated stacks. Anything else is A) not useful
    # information, and B) probably a local build, which could be hanging for
    # much different reasons.
    if percent_symbolicated < 0.8:
        return None

    return (
        tuple(symbolicated),
        key[2],
        key[3],
        key[4],
        key[5],
    )

def process_hang_value(key, val, usage_hours_by_date):
    usage_hours = usage_hours_by_date[key[5]]
    return (val[0] / usage_hours, val[1] / usage_hours)

def get_grouped_sums_and_counts(hangs, processed_modules, usage_hours_by_date, config):
    reduced = (hangs
        .map(lambda hang: map_to_hang_data(hang, config))
        .filter(lambda hang: hang is not None)
        .reduceByKey(merge_hang_data)
        .collect())
    items = [
        (process_hang_key(k, processed_modules), process_hang_value(k, v, usage_hours_by_date))
        for k, v in reduced
    ]
    return [
        k + v for k, v in items if k is not None
    ]

def get_usage_hours(ping):
    build_date = ping["application/buildId"][:8] # "YYYYMMDD" : 8 characters
    usage_hours = float(ping['payload/info/subsessionLength']) / 60.0 / 60.0
    return (build_date, usage_hours)

def merge_usage_hours(a, b):
    return a + b

def get_usage_hours_by_date(pings):
    return (pings.map(get_usage_hours)
        .reduceByKey(merge_usage_hours, REDUCE_BY_KEY_PARALLELISM)
        .collectAsMap())

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

class ThreadFetchSymbolData(threading.Thread):
    def __init__(self, queue, config, result_dict):
        threading.Thread.__init__(self)
        self.queue = queue
        self.config = config
        self.result_dict = result_dict

    def run(self):
        while True:
            try:
                module = self.queue.get(False)
                file_URL = get_file_URL(module, self.config)
                success, response = fetch_URL(file_URL)
                print "Fetched!"
                self.result_dict[module] = (success, response)
            except Queue.Empty:
                return
            except:
                print("Unexpected error:", sys.exc_info()[0])
            finally:
                self.queue.task_done()

def process_modules(stacks_by_module, config):
    print "Fetching {} distinct module URLs...".format(len(stacks_by_module.items()))
    symbol_data = {}
    queue = Queue.Queue()

    for module, offsets in stacks_by_module.iteritems():
        queue.put(module)

    for i in xrange(0,32):
        t = ThreadFetchSymbolData(queue, config, symbol_data)
        t.start()

    queue.join()

    stack_dict = {}

    print "Processing fetched module data..."
    for module, offsets in stacks_by_module.iteritems():
        module_name, breakpad_id = module
        success, response = symbol_data[module]

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

    print "Filtering to hangs with native stacks..."
    hangs = filter_for_hangs_of_type(windows_pings_only)

    print "Getting stacks by module..."
    stacks_by_module = get_stacks_by_module(hangs)
    print "Processing modules..."
    processed_modules = process_modules(stacks_by_module, config)

    print "Getting usage hours..."
    usage_hours_by_date = get_usage_hours_by_date(windows_pings_only)

    print "Grouping stacks..."
    result = get_grouped_sums_and_counts(hangs,
        processed_modules, usage_hours_by_date, config)
    return result

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
        'days_to_aggregate': 21,
        'use_s3': True,
        'sample_size': 0.02,
        'symbol_server_url': "https://s3-us-west-2.amazonaws.com/org.mozilla.crash-stats.symbols-public/v1/",
        'hang_profile_filename': 'hang_profile_128_16000',
        'print_debug_info': False,
        'hang_lower_bound': 128,
        'hang_upper_bound': 16000,
        'stack_acceptance_threshold': 0.0001,
    }

    if config is not None:
        final_config.update(config)

    profile_processor = ProfileProcessor(final_config)
    # We were OOMing trying to allocate a contiguous array for all of this. Pass it in
    # bit by bit to the profile processor and hope it can handle it.
    for x in xrange(2, final_config['days_to_aggregate'] + 2):
        transformed = transform_pings(get_data(sc, final_config, -x, -x), final_config)
        print "Passing stacks to processor..."
        profile_processor.ingest(transformed)
        # Run a collection to ensure that any references to any RDDs are cleaned up,
        # allowing the JVM to clean them up on its end.
        gc.collect()

        print sys.getsizeof(profile_processor)

    profile = profile_processor.process_into_profile()
    write_file(final_config['hang_profile_filename'], profile, final_config)
