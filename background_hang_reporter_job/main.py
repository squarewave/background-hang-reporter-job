
import contextlib
import gc
import gzip
import os
import time
import urllib
import urllib2
import uuid
from bisect import bisect
from datetime import datetime, timedelta
from StringIO import StringIO

import ujson as json
import boto3
from boto3.s3.transfer import S3Transfer
from moztelemetry import get_pings_properties
from moztelemetry.dataset import Dataset

from background_hang_reporter_job.profile import ProfileProcessor
from background_hang_reporter_job.tracked import get_tracking_component
from background_hang_reporter_job.categories import categorize_stack
import background_hang_reporter_job.crashes as crashes

UNSYMBOLICATED = "<unsymbolicated>"
REDUCE_BY_KEY_PARALLELISM = 4001

def deep_merge(original, overrides):
    original_copy = original.copy()
    for k, v in overrides.iteritems():
        if isinstance(v, dict) and k in original_copy and isinstance(original_copy[k], dict):
            original_copy[k] = deep_merge(original_copy[k], v)
        else:
            original_copy[k] = v
    return original_copy

def time_code(name, callback):
    print "{}...".format(name)
    start = time.time()
    result = callback()
    end = time.time()
    delta = end - start
    print "{} took {}ms to complete".format(name, int(round(delta * 1000)))
    return result

def get_data(sc, config, date, end_date=None):
    if config['TMP_use_crashes']:
        return crashes.get_data(sc, config, date)

    if end_date is None:
        end_date = date

    date_str = date.strftime("%Y%m%d")
    end_date_str = end_date.strftime("%Y%m%d")

    doc_type = 'OTHER' if date_str <= '20180315' else 'bhr'
    pings = (Dataset.from_source("telemetry")
             .where(docType=doc_type)
             .where(appBuildId=lambda b: b[:8] >= date_str and b[:8] <= end_date_str)
             .where(appUpdateChannel=config['channel'])
             .records(sc, sample=config['sample_size']))

    if date_str <= '20180315' and end_date_str >= '20180316':
        pings = pings.union(Dataset.from_source("telemetry")
                            .where(docType='bhr')
                            .where(appBuildId=lambda b: b[:8] >= date_str and b[:8] <= end_date_str)
                            .where(appUpdateChannel=config['channel'])
                            .records(sc, sample=config['sample_size']))

    pings = pings.filter(lambda p: p.get('meta', {}).get('docType', {}) == 'bhr')

    if config['exclude_modules']:
        properties = ["environment/system/os/name",
                      "environment/system/os/version",
                      "application/architecture",
                      "application/buildId",
                      "payload/hangs",
                      "payload/timeSinceLastPing"]
    else:
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
    if not isinstance(ping["environment/system/os/version"], basestring):
        return False
    if not isinstance(ping["environment/system/os/name"], basestring):
        return False
    if not isinstance(ping["application/buildId"], basestring):
        return False
    if not isinstance(ping["payload/timeSinceLastPing"], int):
        return False

    return True

def process_frame(frame, modules):
    if isinstance(frame, list):
        if frame[0] < 0 or frame[0] >= len(modules) or frame[0] is None:
            return (None, frame[1])
        return ((modules[frame[0]][0], modules[frame[0]][1]), frame[1])
    else:
        return (('pseudo', None), frame)

def process_hangs(ping):
    build_date = ping["application/buildId"][:8] # "YYYYMMDD" : 8 characters

    os_version_split = ping["environment/system/os/version"].split('.')
    os_version = os_version_split[0] if len(os_version_split) > 0 else ""
    platform = "{}:{}:{}".format(ping["environment/system/os/name"],
                                 os_version,
                                 ping["application/architecture"])

    modules = ping.get('payload/modules', [])
    hangs = ping['payload/hangs']
    if hangs is None:
        return []

    return [(
        [process_frame(frame, modules) for frame in h['stack']],
        h['duration'],
        h['thread'],
        h['runnableName'],
        h['process'],
        h['annotations'],
        build_date,
        platform,
    ) for h in hangs]

def get_all_hangs(pings):
    return pings.flatMap(process_hangs)

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
            .reduceByKey(lambda a, b: a + b, REDUCE_BY_KEY_PARALLELISM)
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
    #pylint: disable=unused-variable
    stack, duration, thread, runnable_name, process, annotations, build_date, platform = hang
    if duration < config['hang_lower_bound']:
        return None
    if duration >= config['hang_upper_bound']:
        return None

    if 'ExternalCPUHigh' in annotations:
        return None

    pending_input = False
    if 'PendingInput' in annotations:
        pending_input = True

    key = (
        tuple((a, b) for a, b in stack),
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
    stack = key[0]
    symbolicated = symbolicate_stacks(stack, processed_modules)

    return (symbolicated,) + key[1:]

def process_hang_value(key, val, usage_hours_by_date):
    #pylint: disable=unused-variable
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
    if lib_name is None or breakpad_id is None:
        return None
    if lib_name.endswith(".pdb"):
        file_name = lib_name[:-4] + ".sym"
    else:
        file_name = lib_name + ".sym"

    try:
        return config['symbol_server_url'] + "/".join([
            urllib.quote_plus(lib_name),
            urllib.quote_plus(breakpad_id),
            urllib.quote_plus(file_name)
        ])
    except KeyError:
        # urllib throws with unicode strings. TODO: investigate why
        # any of these values (lib_name, breakpad_id, file_name) would
        # have unicode strings, or if this is just bad pings.
        return None

def process_module(module, offsets, config):
    result = []
    if module[0] == 'pseudo':
        return [((None, offset), (offset, '')) for offset in offsets]
    file_URL = get_file_URL(module, config)
    module_name, breakpad_id = module
    if file_URL:
        success, response = fetch_URL(file_URL)
    else:
        success = False

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

def map_to_histogram(hang):
    #pylint: disable=unused-variable
    stack, duration, thread, runnable_name, process, annotations, build_date, platform = hang
    category = categorize_stack(stack).split(".")[0]
    component = get_tracking_component(hang)
    hist = [0] * 8
    if duration < 128:
        return (build_date, hist)
    bucket = min(7, int(duration).bit_length() - 8) # 128 will give a bit length of 8
    hist[bucket] = 1
    return ((component, thread, category, build_date), hist)

def reduce_histograms(a, b):
    return [a_bucket + b_bucket for a_bucket, b_bucket in zip(a, b)]

def get_histograms_by_date_thread_category(filtered):
    return (filtered.map(map_to_histogram)
            .reduceByKey(reduce_histograms, REDUCE_BY_KEY_PARALLELISM)
            .collectAsMap())

def count_hangs_in_pings(sc, pings, config):
    filtered = time_code("Filtering to valid pings",
                         lambda: pings.filter(ping_is_valid))

    hangs = time_code("Filtering to hangs with native stacks",
                      lambda: get_all_hangs(filtered))

    frames_by_module = time_code("Getting stacks by module",
                                 lambda: get_frames_by_module(hangs))

    processed_modules = time_code("Processing modules",
                                  lambda: process_modules(sc, frames_by_module, config))

    hangs = hangs.map(lambda hang: process_hang_key(hang, processed_modules))

    usage_hours_by_date = time_code("Getting usage hours",
                                    lambda: get_usage_hours_by_date(filtered))

    histograms = time_code("Getting histograms",
                           lambda: get_histograms_by_date_thread_category(hangs))

    histograms_by_type = []
    all_hangs_histograms_by_thread = {}
    histograms_by_type.append(("All Hangs", all_hangs_histograms_by_thread))
    for title, data in histograms.iteritems():
        histograms_by_thread = {}
        for k, histogram in data.iteritems():
            build_date, thread, category = k
            usage_hours = usage_hours_by_date[build_date]
            for component in [all_hangs_histograms_by_thread, histograms_by_thread]:
                if thread not in component:
                    component[thread] = {}
                if category not in component[thread]:
                    component[thread][category] = {}
                component[thread][category][build_date] = [float(bucket) / usage_hours for bucket in histogram]

        histograms_by_type.append((title, histograms_by_thread))

    return histograms_by_type

def transform_pings(sc, pings, config):
    filtered = time_code("Filtering to valid pings",
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
                       lambda: get_grouped_sums_and_counts(hangs,
                                                           processed_modules,
                                                           usage_hours_by_date, config))
    return result, usage_hours_by_date

def fetch_URL(url):
    result = False, ""
    try:
        with contextlib.closing(urllib2.urlopen(url)) as response:
            #pylint: disable=no-member
            responseCode = response.getcode()
            if responseCode == 404:
                return False, ""
            if responseCode != 200:
                result = False, ""
            return True, decode_response(response)
    except IOError:
        result = False, ""

    if not result[0]:
        try:
            with contextlib.closing(urllib2.urlopen(url)) as response:
                #pylint: disable=no-member
                responseCode = response.getcode()
                if responseCode == 404:
                    return False, ""
                if responseCode != 200:
                    result = False, ""
                return True, decode_response(response)
        except IOError:
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
                #pylint: disable=no-member
                data_stream.seek(0)
                #pylint: disable=no-member
                return data_stream.read().decode('zlib')
    return response.read()

def read_file(name, config):
    end_date = datetime.today()
    end_date_str = end_date.strftime("%Y%m%d")

    if config['read_files_from_network']:
        s3_key = "bhr/data/hang_aggregates/" + name + ".json"
        url = config['analysis_output_url'] + s3_key
        success, response = fetch_URL(url)
        if not success:
            raise Exception('Could not find file at url: ' + url)
        return json.loads(response)
    else:
        if config['append_date']:
            filename = "./output/%s-%s.json" % (name, end_date_str)
        else:
            filename = "./output/%s.json" % name
        gzfilename = filename + '.gz'
        with gzip.open(gzfilename, 'r') as f:
            return json.loads(f.read())

def write_file(name, stuff, config):
    end_date = datetime.today()
    end_date_str = end_date.strftime("%Y%m%d")

    if config['append_date']:
        filename = "./output/%s-%s.json" % (name, end_date_str)
    else:
        filename = "./output/%s.json" % name
    gzfilename = filename + '.gz'
    jsonblob = json.dumps(stuff, ensure_ascii=False)

    if not os.path.exists('./output'):
        os.makedirs('./output')
    with gzip.open(gzfilename, 'w') as f:
        f.write(jsonblob)

    if config['use_s3']:
        bucket = "telemetry-public-analysis-2"
        s3_key = "bhr/data/hang_aggregates/" + name + ".json"
        client = boto3.client('s3', 'us-west-2')
        transfer = S3Transfer(client)
        extra_args = {'ContentType':'application/json', 'ContentEncoding':'gzip'}
        transfer.upload_file(gzfilename,
                             bucket,
                             s3_key,
                             extra_args=extra_args)
        if config['uuid'] is not None:
            s3_uuid_key = "bhr/data/hang_aggregates/" + name + "_" + config['uuid'] + ".json"
            transfer.upload_file(gzfilename,
                                 bucket,
                                 s3_uuid_key,
                                 extra_args=extra_args)

default_config = {
    'start_date': datetime.today() - timedelta(days=9),
    'end_date': datetime.today() - timedelta(days=1),
    'use_s3': True,
    'sample_size': 0.50,
    'symbol_server_url': "https://s3-us-west-2.amazonaws.com/org.mozilla.crash-stats.symbols-public/v1/",
    'hang_profile_in_filename': 'hang_profile_128_16000',
    'hang_profile_out_filename': None,
    'print_debug_info': False,
    'hang_lower_bound': 128,
    'hang_upper_bound': 16000,
    'stack_acceptance_threshold': 0.01,
    'hang_outlier_threshold': 512,
    'append_date': False,
    'channel': 'nightly',
    'analysis_output_url': 'https://analysis-output.telemetry.mozilla.org/',
    'read_files_from_network': False,
    'split_threads_in_out_file': False,
    'use_minimal_sample_table': False,
    'post_sample_size': 1.0,
    'TMP_use_crashes': False,
    'exclude_modules': False,
    'uuid': uuid.uuid4().hex,
}

def print_progress(job_start, iterations, current_iteration,
                   iteration_start, iteration_name):
    iteration_end = time.time()
    iteration_delta = iteration_end - iteration_start
    print "Iteration for {} took {}s".format(iteration_name, int(round(iteration_delta)))
    job_elapsed = iteration_end - job_start
    percent_done = float(current_iteration + 1) / float(iterations)
    projected = job_elapsed / percent_done
    remaining = projected - job_elapsed
    print "Job should finish in {}".format(timedelta(seconds=remaining))

def etl_job(sc, _, config=None):
    """This is the function that will be executed on the cluster"""

    final_config = {}
    final_config.update(default_config)

    if config is not None:
        final_config.update(config)

    if final_config['hang_profile_out_filename'] is None:
        final_config['hang_profile_out_filename'] = final_config['hang_profile_in_filename']

    profile_processor = ProfileProcessor(final_config)

    iterations = (final_config['end_date'] - final_config['start_date']).days
    job_start = time.time()
    current_date = None
    transformed = None
    usage_hours = None
    # We were OOMing trying to allocate a contiguous array for all of this. Pass it in
    # bit by bit to the profile processor and hope it can handle it.
    for x in xrange(0, iterations):
        iteration_start = time.time()
        current_date = final_config['start_date'] + timedelta(days=x)
        data = time_code("Getting data",
                         lambda: get_data(sc, final_config, current_date))
        if data is None:
            continue
        transformed, usage_hours = transform_pings(sc, data, final_config)
        time_code("Passing stacks to processor", lambda: profile_processor.ingest(transformed, usage_hours))
        # Run a collection to ensure that any references to any RDDs are cleaned up,
        # allowing the JVM to clean them up on its end.
        gc.collect()
        print_progress(job_start, iterations, x, iteration_start, x)

    profile = profile_processor.process_into_profile()
    write_file(final_config['hang_profile_out_filename'], profile, final_config)

def etl_job_tracked_stats(sc, _, config=None):
    final_config = {}
    final_config.update(default_config)
    if config is not None:
        final_config.update(config)

    if final_config['hang_profile_out_filename'] is None:
        final_config['hang_profile_out_filename'] = final_config['hang_profile_in_filename']

    data = time_code("Getting data",
                     lambda: get_data(sc, final_config,
                                      final_config['start_date'], final_config['end_date']))
    histograms = count_hangs_in_pings(sc, data, final_config)
    existing = read_file(final_config['hang_profile_out_filename'], final_config)
    existing_dict = {k: v for k, v in existing}
    for i, (title, data) in enumerate(histograms):
        if title in existing_dict:
            histograms[i] = (title, deep_merge(existing_dict[title], data))
            del existing_dict[title]
    for entry in existing_dict.iteritems():
        histograms.append(entry)

    write_file(final_config['hang_profile_out_filename'], histograms, final_config)

def etl_job_incremental_write(sc, _, config=None):
    final_config = {}
    final_config.update(default_config)

    if config is not None:
        final_config.update(config)

    if final_config['hang_profile_out_filename'] is None:
        final_config['hang_profile_out_filename'] = final_config['hang_profile_in_filename']

    iterations = (final_config['end_date'] - final_config['start_date']).days
    job_start = time.time()
    current_date = None
    transformed = None
    usage_hours = None
    for x in xrange(iterations):
        iteration_start = time.time()
        current_date = final_config['start_date'] + timedelta(days=x)
        date_str = current_date.strftime("%Y%m%d")
        data = time_code("Getting data",
                         lambda: get_data(sc, final_config, current_date))
        if data is None:
            continue
        transformed, usage_hours = transform_pings(sc, data, final_config)
        profile_processor = ProfileProcessor(final_config)
        profile_processor.ingest(transformed, usage_hours)
        profile = profile_processor.process_into_profile()
        write_file("%s_incremental_%s" % (final_config['hang_profile_out_filename'], date_str),
                   profile, final_config)
        gc.collect()
        print_progress(job_start, iterations, x, iteration_start, date_str)

def etl_job_incremental_finalize(_, __, config=None):
    final_config = {}
    final_config.update(default_config)

    if config is not None:
        final_config.update(config)

    if final_config['hang_profile_out_filename'] is None:
        final_config['hang_profile_out_filename'] = final_config['hang_profile_in_filename']

    profile_processor = ProfileProcessor(final_config)
    iterations = (final_config['end_date'] - final_config['start_date']).days
    job_start = time.time()
    current_date = None
    for x in xrange(iterations):
        iteration_start = time.time()
        current_date = final_config['start_date'] + timedelta(days=x)
        date_str = current_date.strftime("%Y%m%d")
        profile = read_file("%s_incremental_%s" % (final_config['hang_profile_in_filename'], date_str),
                            final_config)
        profile_processor.ingest_processed_profile(profile)
        gc.collect()
        print_progress(job_start, iterations, x, iteration_start, date_str)

    if final_config['split_threads_in_out_file']:
        profile = profile_processor.process_into_split_profile()
        for files in profile['file_data']:
            for name, data in files:
                write_file(final_config['hang_profile_out_filename'] + '_' + name, data, final_config)
        write_file(final_config['hang_profile_out_filename'], profile['main_payload'], final_config)
    else:
        profile = profile_processor.process_into_profile()
        write_file(final_config['hang_profile_out_filename'], profile, final_config)
