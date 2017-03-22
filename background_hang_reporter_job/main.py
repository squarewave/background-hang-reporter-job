import os
import ujson as json
import pandas as pd
import requests
from datetime import datetime, timedelta

from moztelemetry import get_pings_properties
from moztelemetry.dataset import Dataset

use_s3 = False
days_to_aggregate = 3
sample_size = 0.01
snappy_url = "http://snappy2-zero.herokuapp.com"

def get_data(sc):
    start_date = (datetime.today() - timedelta(days=days_to_aggregate))
    start_date_str = start_date.strftime("%Y%m%d")
    end_date = (datetime.today() - timedelta(days=0))
    end_date_str = end_date.strftime("%Y%m%d")

    pings = (Dataset.from_source("telemetry")
        .where(docType='main')
        .where(appBuildId=lambda b: (b.startswith(start_date_str) or b > start_date_str)
                                     and (b.startswith(end_date_str) or b < end_date_str))
        .where(appUpdateChannel="nightly")
        .records(sc, sample=sample_size))

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
        print stack_tuple
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

def transform_stacks(results):
    memory_map_dict = {}
    for date, threads in pings.iteritems():
        for signature, data in threads.iteritems():
            for stack_info, stats in data.stacks:
                pseudo, memory_map, stack = stack_info
                if memory_map not in memory_map_dict:
                    memory_map_dict[memory_map] = [stack]

    stack_dict = {};
    for memory_map, stacks in memory_map_dict.iteritems():
        payload = {
            'version': 4,
            'memoryMap': memory_map,
            'stacks': stacks
        }

        response = requests.post(snappy_url, data=json.dumps(payload))

        for native, symbolicated in zip(stacks, response['symbolicatedStacks']):
            stack_dict[(memory_map, native)] = symbolicated

    for date, threads in pings.iteritems():
        for signature, data in threads.iteritems():
            data.stacks = [
                stack_dict[memory_map, stack]
                for (pseudo, memory_map, stack), stats in data.stacks
            ]

def write_file(name, stuff):
    filename = "./output/%s-%s.json" % (name, end_date_str)
    jsonblob = json.dumps(stuff, ensure_ascii=False)

    if use_s3:
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

def etl_job(sc, sqlContext):
    """This is the function that will be executed on the cluster"""

    results = transform_pings(get_data(sc))

    transform_stacks(results)

    write_file('pseudostacks_by_day', results)
