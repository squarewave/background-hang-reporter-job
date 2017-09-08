import boto3
import contextlib
import gc
import gzip
import os
import pandas as pd
import Queue
import sys
import threading
import time
import ujson as json
import urllib
import urllib2
import uuid

from bisect import bisect
from boto3.s3.transfer import S3Transfer
from datetime import datetime, timedelta
from moztelemetry import get_pings_properties
from moztelemetry.dataset import Dataset
from sets import Set
from StringIO import StringIO

from profile import ProfileProcessor

UNSYMBOLICATED = "<unsymbolicated>"
REDUCE_BY_KEY_PARALLELISM = 2001

def time_code(name, fn):
    print "{}...".format(name)
    start = time.time()
    result = fn()
    end = time.time()
    delta = end - start
    print "{} took {}ms to complete".format(name, int(round(delta * 1000)))
    return result

def get_data(sc, config, start_date_relative, end_date_relative):
    start_date = (datetime.today() + timedelta(days=start_date_relative))
    start_date_str = start_date.strftime("%Y%m%d")
    end_date = (datetime.today() + timedelta(days=end_date_relative))
    end_date_str = end_date.strftime("%Y%m%d")

    pings = (Dataset.from_source("telemetry")
        .where(docType='OTHER')
        .where(appBuildId=lambda b: (b.startswith(start_date_str) or b > start_date_str)
                                     and (b.startswith(end_date_str) or b < end_date_str))
        .where(appUpdateChannel="nightly")
        .records(sc, sample=config['sample_size']))

    pings = pings.filter(lambda p: p.get('meta', {}).get('docType', {}) == 'bhr')

    properties = ["environment/system/os/name",
                  "environment/system/os/version",
                  "application/architecture",
                  "application/buildId",
                  "payload/modules",
                  "payload/hangs",
                  "payload/timeSinceLastPing"]

    try:
        return get_pings_properties(pings, properties, with_processes=True)
    except ValueError:
        return None

def ping_is_valid(ping):
    if not isinstance(ping["application/buildId"], basestring):
        return False
    if type(ping["payload/timeSinceLastPing"]) != int:
        return False

    return True

def process_stack(stack, modules):
    if isinstance(stack, list):
        if stack[0] == -1 or stack[0] is None:
            return (None, stack[1])
        return ((modules[stack[0]][0], modules[stack[0]][1]), stack[1])
    else:
        return (('pseudo', None), stack)

def process_hangs(ping):
    result = []

    build_date = ping["application/buildId"][:8] # "YYYYMMDD" : 8 characters

    os_version_split = ping["environment/system/os/version"].split('.')
    os_version = os_version_split[0] if len(os_version_split) > 0 else ""
    platform = "{}:{}:{}".format(ping["environment/system/os/name"],
                                 os_version,
                                 ping["application/architecture"])

    modules = ping['payload/modules']
    hangs = ping['payload/hangs']

    return [(
        [process_stack(s, modules) for s in h['stack']],
        h['duration'],
        h['thread'],
        h['runnableName'],
        h['process'],
        h['annotations'],
        build_date,
        platform,
    ) for h in hangs]

def get_all_hangs(pings):
    return pings.flatMap(lambda p: process_hangs(p))

def map_to_frame_info(hang):
    memory_map = hang['hang']['nativeStack']['memoryMap']
    stack = hang['hang']['nativeStack']['stacks'][0]
    return [
        (tuple(memory_map[module_index]), (offset,)) if module_index != -1 else (None, (offset,))
        for module_index, offset in stack
    ]

def get_frames_by_module(hangs):
    return (hangs.flatMap(lambda hang: hang[0])
        .filter(lambda hang_tuple: hang_tuple[0] is not None)
        .map(lambda hang_tuple: (hang_tuple[0], (hang_tuple[1],)))
        .distinct()
        .reduceByKey(lambda a,b: a + b, REDUCE_BY_KEY_PARALLELISM)
        .collectAsMap())

def symbolicate_stacks(stack, processed_modules):
    symbolicated = []
    for module, offset in stack:
        if module is not None:
            debug_name, breakpad_id = module
            processed = processed_modules.get((breakpad_id, offset), None)
            if processed is not None:
                symbolicated.append(processed)
            else:
                symbolicated.append((UNSYMBOLICATED, debug_name))
        else:
            symbolicated.append((UNSYMBOLICATED, 'unknown'))
    return symbolicated

def map_to_hang_data(hang, config):
    stack, duration, thread, runnable_name, process, annotations, build_date, platform = hang
    if duration < config['hang_lower_bound']:
        return None
    if duration >= config['hang_upper_bound']:
        return None

    pending_input = False
    if 'PendingInput' in annotations:
        pending_input = True

    key = (
        tuple((a,b) for a,b in stack),
        runnable_name,
        thread,
        build_date,
        pending_input,
        platform)

    return (key, (
        float(duration),
        1.0,
    ))

def merge_hang_data(a, b):
    return (
        a[0] + b[0],
        a[1] + b[1],
    )

def process_hang_key(key, processed_modules):
    stack, runnable_name, thread, build_date, pending_input, platform = key
    symbolicated = symbolicate_stacks(stack, processed_modules)

    return (
        tuple(symbolicated),
        runnable_name,
        thread,
        build_date,
        pending_input,
        platform,
    )

def process_hang_value(key, val, usage_hours_by_date):
    stack, runnable_name, thread, build_date, pending_input, platform = key
    return (val[0] / usage_hours_by_date[build_date], val[1] / usage_hours_by_date[build_date])

def get_grouped_sums_and_counts(hangs, processed_modules, usage_hours_by_date, config):
    reduced = (hangs
        .map(lambda hang: map_to_hang_data(hang, config))
        .filter(lambda hang: hang is not None)
        .reduceByKey(merge_hang_data, REDUCE_BY_KEY_PARALLELISM)
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
    usage_hours = float(ping["payload/timeSinceLastPing"]) / 3600000.0
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

def process_module(module, offsets, config):
    result = []
    if module[0] == 'pseudo':
        return [((None, offset), (offset, '')) for offset in offsets]
    file_URL = get_file_URL(module, config)
    success, response = fetch_URL(file_URL)
    module_name, breakpad_id = module

    if success:
        sorted_keys, sym_map = make_sym_map(response)

        for offset in offsets:
            i = bisect(sorted_keys, int(offset, 16))
            key = sorted_keys[i - 1] if i else None

            symbol = sym_map.get(key)
            if symbol is not None:
                result.append(((breakpad_id, offset), (symbol, module_name)))
            else:
                result.append(((breakpad_id, offset), (UNSYMBOLICATED, module_name)))
    else:
        for offset in offsets:
            result.append(((breakpad_id, offset), (UNSYMBOLICATED, module_name)))
    return result

def process_modules(sc, frames_by_module, config):
    data = sc.parallelize(frames_by_module.iteritems())
    return data.flatMap(lambda x: process_module(x[0], x[1], config)).collectAsMap()

def transform_pings(sc, pings, config):
    filtered = time_code("Filtering to Windows pings",
        lambda: pings.filter(ping_is_valid))

    hangs = time_code("Filtering to hangs with native stacks",
        lambda: get_all_hangs(filtered))

    frames_by_module = time_code("Getting stacks by module",
        lambda: get_frames_by_module(hangs))

    processed_modules = time_code("Processing modules",
        lambda: process_modules(sc, frames_by_module, config))

    usage_hours_by_date = time_code("Getting usage hours",
        lambda: get_usage_hours_by_date(filtered))

    result = time_code("Grouping stacks",
        lambda: get_grouped_sums_and_counts(hangs, processed_modules, usage_hours_by_date, config))
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
    with gzip.open(gzfilename, 'w') as f:
        f.write(jsonblob)

    if config['use_s3']:
        bucket = "telemetry-public-analysis-2"
        s3_key = "bhr/data/hang_aggregates/" + name + ".json"
        s3_uuid_key = "bhr/data/hang_aggregates/" + name + "_" + config['uuid'] + ".json"
        client = boto3.client('s3', 'us-west-2')
        transfer = S3Transfer(client)
        transfer.upload_file(gzfilename,
                             bucket,
                             s3_key,
                             extra_args={
                                'ContentType':'application/json',
                                'ContentEncoding':'gzip'
                            })
        transfer.upload_file(gzfilename,
                             bucket,
                             s3_uuid_key,
                             extra_args={
                                'ContentType':'application/json',
                                'ContentEncoding':'gzip'
                            })

def etl_job(sc, sqlContext, config=None):
    """This is the function that will be executed on the cluster"""

    final_config = {
        'days_to_aggregate': 8,
        'date_clumping': 1,
        'use_s3': True,
        'sample_size': 0.50,
        'symbol_server_url': "https://s3-us-west-2.amazonaws.com/org.mozilla.crash-stats.symbols-public/v1/",
        'hang_profile_filename': 'hang_profile_128_16000',
        'print_debug_info': False,
        'hang_lower_bound': 128,
        'hang_upper_bound': 16000,
        'stack_acceptance_threshold': 0.01,
        'hang_outlier_threshold': 512,
        'uuid': uuid.uuid4().hex,
    }

    if config is not None:
        final_config.update(config)

    iterations = (final_config['days_to_aggregate'] + final_config['date_clumping'] - 1) / final_config['date_clumping']
    profile_processor = ProfileProcessor(final_config)

    job_start = time.time()
    # We were OOMing trying to allocate a contiguous array for all of this. Pass it in
    # bit by bit to the profile processor and hope it can handle it.
    for x in xrange(0, iterations):
        day_start = time.time()
        rel_end = x * final_config['date_clumping']
        rel_end = -rel_end - 1
        rel_start = min((x + 1) * final_config['date_clumping'] - 1, final_config['days_to_aggregate'])
        rel_start = -rel_start - 1
        data = time_code("Getting data",
            lambda: get_data(sc, final_config, rel_start, rel_end))
        if data is None:
            continue
        transformed = transform_pings(sc, data, final_config)
        time_code("Passing stacks to processor", lambda: profile_processor.ingest(transformed))
        # Run a collection to ensure that any references to any RDDs are cleaned up,
        # allowing the JVM to clean them up on its end.
        gc.collect()
        day_end = time.time()
        day_delta = day_end - day_start
        print "Iteration took {}s".format(int(round(day_delta)))
        job_elapsed = time.time() - job_start
        percent_done = float(x + 1) / float(iterations)
        projected = job_elapsed / percent_done
        remaining = projected - job_elapsed
        print "Job should finish in {}".format(timedelta(seconds=remaining))

    profile = profile_processor.process_into_profile()
    write_file(final_config['hang_profile_filename'], profile, final_config)
