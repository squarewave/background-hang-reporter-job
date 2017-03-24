import os
import ujson as json
import pandas as pd
import urllib
import urllib2
import contextlib
import gzip
from StringIO import StringIO
from sets import Set
from datetime import datetime, timedelta

from moztelemetry import get_pings_properties
from moztelemetry.dataset import Dataset

def get_data(sc, config):
    start_date = (datetime.today() - timedelta(days=config['days_to_aggregate']))
    start_date_str = start_date.strftime("%Y%m%d")
    end_date = (datetime.today() - timedelta(days=0))
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

def only_hangs_of_type(ping):
    build_date = ping["application/buildId"][:8] # "YYYYMMDD" : 8 characters
    usage_hours = float(ping['payload/info/subsessionLength']) / 60.0

    result = []

    if usage_hours == 0.0:
        return result

    if ping['payload/childPayloads'] is not None:
        for payload in ping['payload/childPayloads']:
            if 'threadHangStats' not in payload:
                continue

            for thread_hang in payload['threadHangStats']:
                if 'name' not in thread_hang:
                    continue

                if len(thread_hang['hangs']) > 0:
                    result = result + [
                        {
                            'build_date': build_date,
                            'thread_name': thread_hang['name'],
                            'usage_hours': usage_hours,
                            'hang': x
                        }
                        for x in thread_hang['hangs']
                    ]

    if ping['payload/threadHangStats'] is not None:
        for thread_hang in ping['payload/threadHangStats']:
            if 'name' not in thread_hang:
                continue

            if len(thread_hang['hangs']) > 0:
                result = result + [
                    {
                        'build_date': build_date,
                        'thread_name': thread_hang['name'],
                        'usage_hours': usage_hours,
                        'hang': x
                    }
                    for x in thread_hang['hangs']
                ]

    return result

def filter_for_hangs_of_type(pings):
    return pings.flatMap(lambda p: only_hangs_of_type(p))

def tupleize(l):
    if type(l) is list:
        return tuple(tupleize(x) for x in l)
    else:
        return l

def map_to_hang_data(hang):
    hist_data = hang['hang']['histogram']['values']
    key_ints = map(int, hist_data.keys())
    hist = pd.Series(hist_data.values(), index=key_ints)
    weights = pd.Series(key_ints, index=key_ints)
    hang_sum = (hist * weights)[hist.index >= 100].sum()
    hang_count = hist[hist.index >= 100].sum()

    if 'nativeStack' in hang['hang']:
        stack_tuple = (
            tuple(hang['hang']['stack']),
            tupleize(hang['hang']['nativeStack']['memoryMap']),
            tupleize(hang['hang']['nativeStack']['stacks'][0]),
        )
    else:
        stack_tuple = (
            tuple(hang['hang']['stack']),
            None,
            None
        )

    key = (stack_tuple, hang['thread_name'], hang['build_date'])

    # our key will be the stack, the thread name, and the build ID. Once we've
    # reduced on this we'll collect as a map, since there should only be
    # ~10^1 days, 10^1 threads, 10^3 stacks : 100,000 records
    return (key, {
        'hang_ms_per_hour': hang_sum / hang['usage_hours'],
        'hang_count_per_hour': hang_count / hang['usage_hours'],
    })

def merge_hang_data(a, b):
    return {
        'hang_ms_per_hour': a['hang_ms_per_hour'] + b['hang_ms_per_hour'],
        'hang_count_per_hour': a['hang_count_per_hour'] + b['hang_count_per_hour'],
    }

def get_grouped_sums_and_counts(hangs):
    return (hangs.map(map_to_hang_data)
        .reduceByKey(merge_hang_data)
        .collectAsMap())

def group_by_date(stacks):
    dates = {}
    for key, stats in stacks.iteritems():
        stack, thread_name, build_date = key;

        hang_ms_per_hour = stats['hang_ms_per_hour']
        hang_count_per_hour = stats['hang_count_per_hour']

        if len(stack) == 0:
            continue

        if not build_date in dates:
            dates[build_date] = {
                "date": build_date,
                "threads": [],
            }

        new_key = (stack, thread_name)

        date = dates[build_date]

        date["threads"].append((new_key, {
            'hang_ms_per_hour': hang_ms_per_hour,
            'hang_count_per_hour': hang_count_per_hour
        }))

    return dates

def group_by_thread_name(stacks):
    thread_names = {}
    for key, stats in stacks:
        new_key, thread_name = key

        hang_ms_per_hour = stats['hang_ms_per_hour']
        hang_count_per_hour = stats['hang_count_per_hour']

        if not thread_name in thread_names:
            thread_names[thread_name] = {
                "thread": thread_name,
                "hangs": [],
            }

        thread_name_obj = thread_names[thread_name]

        thread_name_obj["hangs"].append((new_key, {
            'hang_ms_per_hour': hang_ms_per_hour,
            'hang_count_per_hour': hang_count_per_hour
        }))

    return thread_names

def group_by_top_frame(stacks):
    top_frames = {}
    for stack, stats in stacks:
        hang_ms_per_hour = stats['hang_ms_per_hour']
        hang_count_per_hour = stats['hang_count_per_hour']

        if len(stack[0]) == 0:
            stack_top_frame = 'empty_pseudo_stack'
        else:
            stack_top_frame = stack[0][-1]

        if not stack_top_frame in top_frames:
            top_frames[stack_top_frame] = {
                "stacks": [],
                "hang_ms_per_hour": 0,
                "hang_count_per_hour": 0
            }

        top_frame = top_frames[stack_top_frame]

        top_frame["stacks"].append((stack, {
            'hang_ms_per_hour': hang_ms_per_hour,
            'hang_count_per_hour': hang_count_per_hour
        }))
        top_frame["stacks"] = sorted(top_frame["stacks"],
                                     key=lambda s: -s[1]['hang_count_per_hour'])

        top_frame["hang_ms_per_hour"] += hang_ms_per_hour
        top_frame["hang_count_per_hour"] += hang_count_per_hour

    return top_frames

def score(grouping):
    scored_stacks = []
    for stack_tuple in grouping['stacks']:
        scored_stacks.append((stack_tuple[0], {
            'hang_ms_per_hour': stack_tuple[1]['hang_ms_per_hour'],
            'hang_count_per_hour': stack_tuple[1]['hang_count_per_hour']
        }))

    grouping['stacks'] = scored_stacks
    return grouping

def score_all(grouped_by_top_frame):
    return {k: score(g) for k, g in grouped_by_top_frame.iteritems()}

def get_by_top_frame_by_thread(by_thread):
    return {
        k: score_all(group_by_top_frame(g["hangs"]))
        for k, g in by_thread.iteritems()
    }

def get_by_thread_by_date(by_date):
    return {
        k: get_by_top_frame_by_thread(group_by_thread_name(g["threads"]))
        for k, g in by_date.iteritems()
    }

def transform_pings(pings):
    windows_pings_only = pings.filter(windows_only)

    hangs = filter_for_hangs_of_type(windows_pings_only)
    grouped_hangs = get_grouped_sums_and_counts(hangs)
    by_date = group_by_date(grouped_hangs)
    scored = get_by_thread_by_date(by_date)

    return scored

def enumerate_threads(results):
    for date, threads in results.iteritems():
        for thread in threads.iteritems():
            # TODO: is there an equivalent to JS's "yield*" in Python?
            yield thread

def enumerate_signatures(results):
    for thread_name, signatures in enumerate_threads(results):
        for signature_data in signatures.iteritems():
            yield signature_data

def enumerate_stacks(results):
    for signature, data in enumerate_signatures(results):
        for stack_data in data['stacks']:
            yield stack_data

def get_stacks_by_module(results):
    breakpad_dict = {}
    for stack_info, stats in enumerate_stacks(results):
        pseudo, memory_map, stack = stack_info
        if memory_map is not None:
            for i, (module_name, breakpad_id) in enumerate(memory_map):
                if breakpad_id not in breakpad_dict:
                    breakpad_dict[module_name, breakpad_id] = Set()
                for stack_module_index, offset in stack:
                    if stack_module_index == i:
                        breakpad_dict[module_name, breakpad_id].add(offset)

    return breakpad_dict

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

def get_file_url(module, config):
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

def get_key(val, sorted_keys):
    if len(sorted_keys) == 0:
        return None

    # simple binary search for biggest key less than val
    search_range = (0, len(sorted_keys))

    while True:
        index = (search_range[0] + search_range[1]) / 2
        if index + 1 == len(sorted_keys):
            return sorted_keys[index]
        if sorted_keys[index] <= val and sorted_keys[index + 1] > val:
            return sorted_keys[index]
        elif sorted_keys[index] > val:
            search_range = (search_range[0], index)
        else:
            search_range = (index+1, search_range[1])

def fetch_URL(url):
    try:
        with contextlib.closing(urllib2.urlopen(url)) as response:
            responseCode = response.getcode()
            if responseCode == 404:
                return False, ""
            if responseCode != 200:
                return False, ""
            return True, decode_response(response)
    except IOError as e:
        pass

    return False, ""

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

def process_modules(stacks_by_module, config):
    stack_dict = {}

    for module, offsets in stacks_by_module.iteritems():
        file_url = get_file_url(module, config)

        module_name, breakpad_id = module

        success, response = fetch_URL(file_url)

        if success:
            sorted_keys, sym_map = make_sym_map(response)

            for offset in offsets:
                key = get_key(offset, sorted_keys)

                symbol = sym_map.get(key)
                if symbol is not None:
                    stack_dict[breakpad_id, offset] = "{} (in {})".format(symbol, module_name)
                else:
                    stack_dict[breakpad_id, offset] = "{} (in {})".format(hex(offset), module_name)
        else:
            for offset in offsets:
                stack_dict[breakpad_id, offset] = "{} (in {})".format(hex(offset), module_name)

    return stack_dict

def apply_processed_modules(results, stack_dict):
    for signature, data in enumerate_signatures(results):
        data_stacks = []
        for (pseudo, memory_map, stack), stats in data['stacks']:
            symbolicated = None
            if memory_map is not None:
                symbolicated = [
                    stack_dict.get((memory_map[module_index][1], offset), offset)
                    if module_index != -1 else offset
                    for module_index, offset in stack
                ]
            data_stacks.append(((pseudo, symbolicated), stats))
        data['stacks'] = data_stacks

def symbolicate_stacks(results, config):
    modules = get_stacks_by_module(results)
    stack_dict = process_modules(modules, config)
    apply_processed_modules(results, stack_dict)

def write_file(name, stuff, config):
    end_date = datetime.today()
    end_date_str = end_date.strftime("%Y%m%d")

    filename = "./output/%s-%s.json" % (name, end_date_str)
    jsonblob = json.dumps(stuff, ensure_ascii=False)

    if config['use_s3']:
        # TODO: This was adapted from another report. I'm not actually sure
        # what the process is for dumping stuff to s3, and would appreciate
        # feedback!
        bucket = "telemetry-public-analysis-2"
        timestamped_s3_key = "bhr/data/hang_aggregates/" + name + ".json"
        client = boto3.client('s3', 'us-west-2')
        transfer = S3Transfer(client)
        transfer.upload_file(filename,
                             bucket,
                             timestamped_s3_key,
                             extra_args={'ContentType':'application/json'})
    else:
        if not os.path.exists('./output'):
            os.makedirs('./output')
        with open(filename, 'w') as f:
            f.write(jsonblob)

def etl_job(sc, sqlContext, config=None):
    """This is the function that will be executed on the cluster"""

    final_config = {
        'days_to_aggregate': 30,
        'use_s3': True,
        'sample_size': 1.0,
        'symbol_server_url': "https://s3-us-west-2.amazonaws.com/org.mozilla.crash-stats.symbols-public/v1/"
    }

    if config is not None:
        final_config.update(config)

    results = transform_pings(get_data(sc, final_config))

    symbolicate_stacks(results, final_config)

    write_file('pseudostacks_by_day', results, final_config)
