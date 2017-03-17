import pytest

import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
from background_hang_reporter_job import *

# allows us to check deep equality while accounting for floating point error
def assert_deep_equality(actual, expected):
    t = type(actual)
    te = type(expected)

    if t is dict and te is dict:
        assert len(actual) == len(expected)
        for k, v in actual.iteritems():
            assert k in expected
            assert_deep_equality(v, expected[k])
    elif (t is list or t is tuple) and (t == te):
        assert len(actual) == len(expected)
        for a,e in zip(actual, expected):
            assert_deep_equality(a, e)
    elif te is float:
        epsilon = 0.000001
        assert abs(actual - expected) < epsilon
    else:
        assert actual == expected

# Initialize a spark context:
@pytest.fixture(scope="session")
def spark_context(request):
    conf = SparkConf().setMaster("local")\
        .setAppName("background_hang_reporter_job" + "_test")
    sc = SparkContext(conf=conf)

    # teardown
    request.addfinalizer(lambda: sc.stop())

    return sc

def create_parent_ping(os, build_id, subsession_length, thread_hang_stats):
    hang_stats = []
    for stat in thread_hang_stats:
        thread_name, stacks, histograms = stat

        hang_stats.append({
            'name': thread_name,
            'hangs': [
                {
                    'histogram': {'values': {'64': h[0], '128': h[1], '256': h[2]}},
                    'stack': s
                } for s, h in zip(stacks, histograms)
            ]
        })

    return {
        'environment/system/os/name': os,
        'application/buildId': build_id,
        'payload/info/subsessionLength': subsession_length,
        'payload/childPayloads': [],
        'payload/threadHangStats': hang_stats
    }

def create_child_ping(os, build_id, subsession_length, child_stats):
    child_payloads = []
    for child_stat in child_stats:
        hang_stats = []

        for thread_name, stacks, histograms in child_stat:

            hang_stats.append({
                'name': thread_name,
                'hangs': [
                    {
                        'histogram': {'values': {'64': h[0], '128': h[1], '256': h[2]}},
                        'stack': s
                    } for s, h in zip(stacks, histograms)
                ]
            })
        child_payloads.append({'threadHangStats': hang_stats})

    return {
        'environment/system/os/name': os,
        'application/buildId': build_id,
        'payload/info/subsessionLength': subsession_length,
        'payload/childPayloads': child_payloads,
        'payload/threadHangStats': []
    }

# builds
# --------------
b_1 = '20170317987654321'
# b_2 is on the same day as build 1
b_2 = '20170317123456789'
# b_3 is on a different day
b_3 = '20170316123456789'

# stacks
# --------------
s_1 = ['asdf:123', 'qwert:4321']
# stack 2 has the same top frame as stack 1 (qwert:4321)
s_2 = ['fdsa:123', 'qwert:4321']
# stack 3 has a different top frame
s_3 = ['asdf:123', 'jklmn:4321']
# stack 4 has the same top frame as stack 3 (jklmn:4321)
s_4 = ['fdsa:123', 'jklmn:4321']

# threads
# --------------
t_1 = 'Gecko'
t_2 = 'Gecko_Child'
t_3 = 'NotGecko1'
t_4 = 'NotGecko2'

windows = 'Windows_NT'
not_windows = 'linux'

def simple_data():
    raw_data = [
        (windows, b_1, 100, [(t_1, [s_1, s_2], [(1, 2, 3), (3, 2, 1)]), (t_4, [s_1, s_2], [(1, 2, 3), (3, 2, 1)])]), # second thread (t_4) should be in a different thread
        (windows, b_2, 200, [(t_1, [s_3, s_2], [(1, 3, 2), (3, 4, 5)])]),
        (not_windows, b_2, 200, [(t_1, [s_3, s_2], [(20, 30, 40), (30, 40, 50)])]), # should be ignored
        (windows, b_3, 100, [(t_1, [s_1, s_2], [(1, 2, 3), (3, 2, 1)])]), # should be in a different date
        (windows, b_1, 100, [(t_3, [s_1, s_2], [(1, 2, 3), (3, 2, 1)])]), # should be in a different thread
    ]

    return map(lambda raw: create_parent_ping(*raw), raw_data)

def child_payloads_data():
    raw_data = [
        (windows, b_1, 100, [[(t_2, [s_1, s_2], [(1, 2, 3), (3, 2, 1)])]]),
        (windows, b_2, 200, [[(t_2, [s_2, s_3], [(3, 2, 3), (1, 2, 1)])], [(t_2, [s_3, s_2], [(3, 1, 1), (4, 2, 2)])]]),
        (not_windows, b_2, 200, [[(t_2, [s_3, s_2], [(20, 30, 40), (30, 40, 50)])]]), # should be ignored
        (windows, b_3, 100, [[(t_2, [s_1, s_2], [(1, 2, 3), (3, 2, 1)])]]), # should be in a different date
        (windows, b_1, 100, [[(t_3, [s_1, s_2], [(1, 2, 3), (3, 2, 1)])]]), # should be in a different thread
    ]

    return map(lambda raw: create_child_ping(*raw), raw_data)

@pytest.fixture
def simple_rdd(spark_context):
    return spark_context.parallelize(simple_data())

@pytest.fixture
def child_rdd(spark_context):
    return spark_context.parallelize(child_payloads_data())

# Tests
def test_simple_transform(simple_rdd):
    transformed = transform_pings(simple_rdd)
    actual = transformed['20170317']['Gecko']

    assert '20170316' in transformed
    assert 'NotGecko1' in transformed['20170317']
    assert 'NotGecko2' in transformed['20170317']

    expected = {
        'qwert:4321': {
            'stacks': [
                (('fdsa:123', 'qwert:4321'), {
                    'hang_ms_per_hour': 307.20 + 537.60, # (2 * 128 + 1 * 256) / (100 / 60) + (4 * 128 + 5 * 256) / (200 / 60)
                    'hang_count_per_hour': 1.80 + 2.70 # (2 + 1) / (100 / 60) + (4 + 5) / (200 / 60)
                }),
                (('asdf:123', 'qwert:4321'), {
                    'hang_ms_per_hour': 614.40, # (2 * 128 + 3 * 256) / (100 / 60)
                    'hang_count_per_hour': 3.00 # (2 + 3) / (100 / 60)
                }),
            ],
            'hang_ms_per_hour': 307.20 + 537.60 + 614.40,
            'hang_count_per_hour': 1.80 + 2.70 + 3.00,
        },
        'jklmn:4321': {
            'stacks': [
                (('asdf:123', 'jklmn:4321'), {
                    'hang_ms_per_hour': 268.80, # (3 * 128 + 2 * 256) / (200 / 60)
                    'hang_count_per_hour': 1.50 # (3 + 2) / (200 / 60)
                }),
            ],
            'hang_ms_per_hour': 268.80, # (3 * 128 + 2 * 256) / (200 / 60)
            'hang_count_per_hour': 1.50 # (3 + 2) / (200 / 60)
        }
    }

    assert_deep_equality(actual, expected)

def test_child_transform(child_rdd):
    transformed = transform_pings(child_rdd)
    print transformed
    actual = transformed['20170317']['Gecko_Child']

    assert '20170316' in transformed
    assert 'NotGecko1' in transformed['20170317']

    # kept all the numbers the same, since the second child entry is the same
    # as the second parent entry, just broken into to processes
    expected = {
        'qwert:4321': {
            'stacks': [
                (('fdsa:123', 'qwert:4321'), {
                    'hang_ms_per_hour': 307.20 + 537.60, # (2 * 128 + 1 * 256) / (100 / 60) + (4 * 128 + 5 * 256) / (200 / 60)
                    'hang_count_per_hour': 1.80 + 2.70 # (2 + 1) / (100 / 60) + (4 + 5) / (200 / 60)
                }),
                (('asdf:123', 'qwert:4321'), {
                    'hang_ms_per_hour': 614.40, # (2 * 128 + 3 * 256) / (100 / 60)
                    'hang_count_per_hour': 3.00 # (2 + 3) / (100 / 60)
                }),
            ],
            'hang_ms_per_hour': 307.20 + 537.60 + 614.40,
            'hang_count_per_hour': 1.80 + 2.70 + 3.00,
        },
        'jklmn:4321': {
            'stacks': [
                (('asdf:123', 'jklmn:4321'), {
                    'hang_ms_per_hour': 268.80, # (3 * 128 + 2 * 256) / (200 / 60)
                    'hang_count_per_hour': 1.50 # (3 + 2) / (200 / 60)
                }),
            ],
            'hang_ms_per_hour': 268.80, # (3 * 128 + 2 * 256) / (200 / 60)
            'hang_count_per_hour': 1.50 # (3 + 2) / (200 / 60)
        }
    }

    assert_deep_equality(actual, expected)
