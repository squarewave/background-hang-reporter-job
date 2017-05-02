import re
import math

PROFILE_INTERVAL = 0.02

tid = 1
pid = 1
fake_start = 1754660864

meta = {
  'interval': PROFILE_INTERVAL,
  'version': 5,
  'stackwalk': 1,
  'debug': 0,
  'gcpoison': 1,
  'asyncstack': 0,
  'startTime': fake_start,
  'processType': 0,
  'platform': 'Windows',
  'oscpu': 'Windows NT 10.0; Win64; x64',
  'misc': 'rv:55.0',
  'abi': 'x86_64-msvc',
  'toolkit': 'windows',
  'product': 'Firefox',
  'preprocessedProfileVersion': 4
}

tasktracer = {
  'taskTable': {
    'length': 0,
    'dispatchTime': [],
    'sourceEventId': [],
    'sourceEventType': [],
    'parentTaskId': [],
    'beginTime': [],
    'processId': [],
    'threadIndex': [],
    'endTime': [],
    'ipdlMsg': [],
    'label': [],
    'address': []
  },
  'tasksIdToTaskIndexMap': {},
  'addressTable': {
    'length': 0,
    'address': [],
    'className': [],
    'lib': []
  },
  'addressIndicesByLib': {},
  'threadTable': {
    'length': 0,
    'tid': [],
    'name': [],
    'start': []
  },
  'tidToThreadIndexMap': {},
  'stringArray': []
}

def to_struct_of_arrays(a):
    if len(a) == 0:
        raise Exception('Need at least one item in array for this to work.')

    result = {k:[e[k] for e in a] for k in a[0].keys()}
    result['length'] = len(a)
    return result

class UniqueKeyedTable:
    def __init__(self, get_default_from_key):
        self.get_default_from_key = get_default_from_key
        self.key_to_index_map = {}
        self.items = []

    def key_to_index(self, key):
        string_key = self.get_dict_key(key)
        if string_key in self.key_to_index_map:
            return self.key_to_index_map[string_key]

        index = len(self.items)
        self.items.append(self.get_default_from_key(key))
        self.key_to_index_map[string_key] = index
        return index

    def key_to_item(self, key):
        return self.items[self.key_to_index(key)]

    def index_to_item(self, index):
        self.items[index]

    def get_items(self):
        return self.items

    def struct_of_arrays(self):
        return to_struct_of_arrays(self.items)

    def get_dict_key(self, key):
        # Only supports one level of nesting
        if type(key) is dict:
            return tuple(key[k] for k in sorted(key.keys()))
        return key

def hexify(num):
    return "{0:#0{1}x}".format(num, 8)

fake_breakpad_id_base = "D746BAF2F0C04D5E9781C9CC9"
breakpad_id_suffix_start = 0

def next_breakpad_id(key):
    global breakpad_id_suffix_start
    breakpad_id_suffix_start += 1
    return fake_breakpad_id_base + hexify(breakpad_id_suffix_start)

breakpad_id_table = UniqueKeyedTable(next_breakpad_id)

def get_default_lib(name):
    global fake_start
    start = fake_start + 1
    end = fake_start + 10000
    fake_start += 10000
    return ({
        'name': re.sub(r'\.pdb$', '', name),
        'start': start,
        'end': end,
        'offset': 0,
        'path': "",
        'debugName': name,
        'debugPath': name,
        'breakpadId': breakpad_id_table.key_to_item(name),
        'arch': "",
    })

def get_default_thread(name):
    strings_table = UniqueKeyedTable(lambda str: str)
    libs = UniqueKeyedTable(get_default_lib)
    resource_table = UniqueKeyedTable(lambda name: ({
        'type': 1,
        'name': strings_table.key_to_index(name),
        'lib': libs.key_to_index(name)
    }))
    func_table = UniqueKeyedTable(lambda key: ({
        'name': strings_table.key_to_index(key['name']),
        'resource': None if key['lib'] is None else resource_table.key_to_index(key['lib']),
        'address': -1,
        'isJS': False,
        'fileName': None,
        'lineNumber': None,
    }))
    frame_table = UniqueKeyedTable(lambda key: ({
        'implementation': None,
        'optimizations': None,
        'line': None,
        'category': None,
        'func': func_table.key_to_index({'name': key['name'], 'lib': key['lib']}),
        'address': -1,
    }))
    stack_table = UniqueKeyedTable(lambda key: ({
        'prefix': key['prefix'],
        'frame': frame_table.key_to_index({'name': key['name'], 'lib': key['lib']}),
    }))

    stack_table.key_to_index({'name': '(root)', 'lib': None, 'prefix': None})

    global tid
    global pid

    tid += 1
    pid += 1
    return {
        'name': name,
        'libs': libs,
        'frameTable': frame_table,
        'funcTable': func_table,
        'resourceTable': resource_table,
        'stackTable': stack_table,
        'stringArray': strings_table,
        'processType': 'default',
        'tid': tid,
        'pid': pid,
        'time': 0.0,
        'markers': {
            'length': 0,
            'name': [],
            'time': [],
            'data': [],
        },
        'samples': {
            'length': 0,
            'stack': [],
            'time': [],
            'responsiveness': [],
            'rss': [],
            'uss': [],
            'frameNumber': [],
        },
        'samples': [],
    }

def process_thread(thread):
    result = {
        'name': thread['name'],
        'processType': thread['processType'],
        'tid': thread['tid'],
        'pid': thread['pid'],
        'libs': thread['libs'].get_items(),
        'frameTable': thread['frameTable'].struct_of_arrays(),
        'funcTable': thread['funcTable'].struct_of_arrays(),
        'resourceTable': thread['resourceTable'].struct_of_arrays(),
        'stackTable': thread['stackTable'].struct_of_arrays(),
        'markers': thread['markers'],
        'samples': to_struct_of_arrays(thread['samples']),
        'stringArray': thread['stringArray'].get_items(),
    }
    result['resourceTable']['icons'] = []
    result['resourceTable']['addonId'] = []
    result['resourceTable']['host'] = []
    return result

def get_sort_key(row):
    stack, thread_name, build_date, hang_ms, hangCount = row
    return (build_date,) + tuple(stack)

def process_into_profile(data):
    thread_table = UniqueKeyedTable(get_default_thread)

    for row in sorted(data, key=get_sort_key):
        stack, thread_name, build_date, hang_ms, hangCount = row

        thread = thread_table.key_to_item(thread_name)
        stack_table = thread['stackTable']
        samples = thread['samples']

        last_stack = 0
        for frame in reversed(stack):
            cpp_match = (
                re.search(r'^(.*) \(in ([^)]*)\) (\+ [0-9]+)$', frame) or
                re.search(r'^(.*) \(in ([^)]*)\) (\(.*:.*\))$', frame) or
                re.search(r'^(.*) \(in ([^)]*)\)$', frame)
            )
            if cpp_match:
                func_name = cpp_match.group(1);
                lib_name = cpp_match.group(2);
            else:
                func_name = frame;
                lib_name = 'unknown';

            last_stack = stack_table.key_to_index({'name': func_name, 'lib': lib_name, 'prefix': last_stack});

        # The interval must be tuned to a value that helps this approximate the
        # actual hang time.
        for i in range(0, int(math.ceil(hang_ms / PROFILE_INTERVAL))):
            thread['time'] += PROFILE_INTERVAL
            samples.append({
              'stack': last_stack,
              'time': thread['time'],
              'responsiveness': PROFILE_INTERVAL,
              'rss': None,
              'uss': None,
              'frameNumber': None,
            })

    return {
        'meta': meta,
        'tasktracer': tasktracer,
        'threads': [process_thread(t) for t in thread_table.get_items()],
    }
