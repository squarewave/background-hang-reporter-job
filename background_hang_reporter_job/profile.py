import re
import math

tid = 1
pid = 1
fake_start = 1754660864

def to_struct_of_arrays(a):
    if len(a) == 0:
        raise Exception('Need at least one item in array for this to work.')

    result = {k:[e[k] for e in a] for k in a[0].keys()}
    result['length'] = len(a)
    return result

class UniqueKeyedTable:
    def __init__(self, get_default_from_key, key_names = ()):
        self.get_default_from_key = get_default_from_key
        self.key_to_index_map = {}
        self.key_names = key_names
        self.items = []

    def key_to_index(self, key):
        if key in self.key_to_index_map:
            return self.key_to_index_map[key]

        index = len(self.items)
        self.items.append(self.get_default_from_key(key))
        self.key_to_index_map[key] = index
        return index

    def key_to_item(self, key):
        return self.items[self.key_to_index(key)]

    def index_to_item(self, index):
        return self.items[index]

    def get_items(self):
        return self.items

    def struct_of_arrays(self):
        if len(self.items) == 0:
            raise Exception('Need at least one item in array for this to work.')

        result = {}
        num_keys = len(self.key_names)
        for i in xrange(0, num_keys):
            result[self.key_names[i]] = [x[i] for x in self.items]

        result['length'] = len(self.items)
        return result

    def sorted_struct_of_arrays(self, key):
        return to_struct_of_arrays(sorted(self.items, key=key))

class GrowToFitList(list):
    def __setitem__(self, index, value):
        if index >= len(self):
            to_grow = index + 1 - len(self)
            self.extend([None] * to_grow)
        list.__setitem__(self, index, value)

    def __getitem__(self, index):
        if index >= len(self):
            return None
        return list.__getitem__(self, index)

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
    func_table = UniqueKeyedTable(lambda key: (
        strings_table.key_to_index(key[0]),
        None if key[1] is None else libs.key_to_index(key[1])
    ), ('name', 'lib'))
    stack_table = UniqueKeyedTable(lambda key: (
        key[2],
        func_table.key_to_index((key[0], key[1]))
    ), ('prefix', 'func'))
    sample_table = UniqueKeyedTable(lambda key: (
        key[0],
        strings_table.key_to_index(key[1]),
        key[2]
    ), ('stack', 'runnable', 'userInteracting'))
    pseudo_stack_table = UniqueKeyedTable(lambda key: (
        key[1],
        func_table.key_to_index((key[0], None))
    ), ('prefix', 'func'))
    stack_to_pseudo_stacks_table = UniqueKeyedTable(lambda key: [
        key[0],
        key[1],
        0.0,
        0.0
    ], ('stack', 'pseudoStack', 'stackHangMs', 'stackHangCount'))

    stack_table.key_to_index(('(root)', None, None))
    pseudo_stack_table.key_to_index(('(root)', None))

    global tid
    global pid

    tid += 1
    pid += 1
    return {
        'name': name,
        'libs': libs,
        'funcTable': func_table,
        'stackTable': stack_table,
        'sampleTable': sample_table,
        'pseudoStackTable': pseudo_stack_table,
        'stackToPseudoStacksTable': stack_to_pseudo_stacks_table,
        'stringArray': strings_table,
        'processType': 'tab' if name == 'Gecko_Child' or name == 'Gecko_Child_ForcePaint' else 'default',
        'tid': tid,
        'pid': pid,
        'time': 0.0,
        'dates': UniqueKeyedTable(lambda date: ({
            'date': date,
            'sampleHangMs': GrowToFitList(),
            'sampleHangCount': GrowToFitList()
        })),
    }

def process_thread(thread):
    return {
        'name': thread['name'],
        'processType': thread['processType'],
        'tid': thread['tid'],
        'pid': thread['pid'],
        'libs': thread['libs'].get_items(),
        'funcTable': thread['funcTable'].struct_of_arrays(),
        'stackTable': thread['stackTable'].struct_of_arrays(),
        'sampleTable': thread['sampleTable'].struct_of_arrays(),
        'pseudoStackTable': thread['pseudoStackTable'].struct_of_arrays(),
        'stackToPseudoStacksTable': thread['stackToPseudoStacksTable'].sorted_struct_of_arrays(lambda r: r['stack']),
        'dates': thread['dates'].get_items(),
        'stringArray': thread['stringArray'].get_items(),
    }

class ProfileProcessor:
    def __init__(self, config):
        self.config = config
        self.thread_table = UniqueKeyedTable(get_default_thread)

    def debugDump(self, str):
        if self.config['print_debug_info']:
            print str

    def ingest(self, data):
        print "{} unfiltered samples in data".format(len(data))
        data = [
            x
            for x in data
            # x[6] should be hang_ms
            if x[6] > 0.0
        ]
        print "{} filtered samples in data".format(len(data))

        prune_stack_cache = UniqueKeyedTable(lambda key: [0.0])
        root_stack = prune_stack_cache.key_to_item(('(root)', None, None))

        print "Preprocessing stacks for prune cache..."
        for row in data:
            stack, pseudo, runnable_name, thread_name, submission_date, user_interacting, hang_ms, hang_count = row

            root_stack[0] += hang_ms

            last_stack = 0
            for (func_name, lib_name) in stack:
                cache_item = prune_stack_cache.key_to_item((func_name, lib_name, last_stack))
                last_stack = prune_stack_cache.key_to_index((func_name, lib_name, last_stack))
                cache_item[0] += hang_ms

        print "Processing stacks..."
        for row in data:
            stack, pseudo, runnable_name, thread_name, submission_date, user_interacting, hang_ms, hang_count = row

            thread = self.thread_table.key_to_item(thread_name)
            stack_table = thread['stackTable']
            sample_table = thread['sampleTable']
            pseudo_stack_table = thread['pseudoStackTable']
            stack_to_pseudo_stacks_table = thread['stackToPseudoStacksTable']
            dates = thread['dates']

            last_stack = 0
            last_cache_item_index = 0
            last_lib_name = None
            for (func_name, lib_name) in stack:
                cache_item_index = prune_stack_cache.key_to_index((func_name, lib_name, last_cache_item_index))
                cache_item = prune_stack_cache.index_to_item(cache_item_index)
                if cache_item[0] / root_stack[0] > self.config['stack_acceptance_threshold']:
                    last_lib_name = lib_name
                    last_stack = stack_table.key_to_index((func_name, lib_name, last_stack))
                    last_cache_item_index = cache_item_index
                else:
                    self.debugDump("Stripping stack {} - {} / {}".format(func_name,
                        cache_item[0], root_stack[0]))
                    break

            sample_index = sample_table.key_to_index((last_stack, runnable_name, user_interacting))

            date = dates.key_to_item(submission_date)
            if date['sampleHangMs'][sample_index] is None:
                date['sampleHangMs'][sample_index] = 0.0
                date['sampleHangCount'][sample_index] = 0

            date['sampleHangMs'][sample_index] += hang_ms
            date['sampleHangCount'][sample_index] += hang_count

            last_pseudo = 0
            for frame in pseudo:
                last_pseudo = pseudo_stack_table.key_to_index((frame, last_pseudo))

            stack_to_pseudo_stack = stack_to_pseudo_stacks_table.key_to_item((last_stack, last_pseudo))

            stack_to_pseudo_stack[2] += hang_ms
            stack_to_pseudo_stack[3] += hang_count

    def process_into_profile(self):
        print "Processing into final format..."
        return {
            'threads': [process_thread(t) for t in self.thread_table.get_items()],
            'uuid': self.config['uuid'],
        }
