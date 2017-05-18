import re
import math

tid = 1
pid = 1
fake_start = 1754660864

stack_condense_groups = [
    # TODO: this list is a work in progress
    ('(script)', [
        r'^js::',
        r'^JS::',
        r'^JS_',
        r'^Compile$',
        r'^BytecodeCompiler::',
        r'^SharedStub$',
        r'^DefinePropertyById$',
        r'^DefineSelfHostedProperty$',
        r'^mozJSComponentLoader::',
        r'^XPCWrappedNative::',
        r'^XPCWrappedNativeProto::',
        r'^nsXPCWrappedJS::',
        r'^nsXPCWrappedJSClass::',
        r'^DefineSelfHostedProperty$',
        r'^JSFunction::',
        r'^JSObject::',
        r'^InternalCall$',
        r'^InternalConstruct$',
        r'^GetNameOperation$',
        r'^SetPropertyOperation$',
        r'^Interpret$',
        r'^XPC_WN_CallMethod',
        r'^XPCWrappedNative::CallMethod',
        r'^XPCConvert::',
        r'^date_parse$',
        r'^json_parse$',
        r'^xpc::',
        r'^NumberFormat$',
        r'^LegacyIntlInitialize$',
    ]),
    ('(unsymbolicated)', [
        r'<unsymbolicated>'
    ]),
]

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

    def sorted_struct_of_arrays(self, key):
        return to_struct_of_arrays(sorted(self.items, key=key))

    def get_dict_key(self, key):
        # Only supports one level of nesting
        if type(key) is dict:
            return tuple(key[k] for k in sorted(key.keys()))
        return key

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
    func_table = UniqueKeyedTable(lambda key: ({
        'name': strings_table.key_to_index(key['name']),
        'lib': None if key['lib'] is None else libs.key_to_index(key['lib']),
    }))
    stack_table = UniqueKeyedTable(lambda key: ({
        'prefix': key['prefix'],
        'func': func_table.key_to_index({'name': key['name'], 'lib': key['lib']}),
    }))
    pseudo_stack_table = UniqueKeyedTable(lambda key: ({
        'prefix': key['prefix'],
        'func': func_table.key_to_index({'name': key['name'], 'lib': None}),
    }))
    stack_to_pseudo_stacks_table = UniqueKeyedTable(lambda key: ({
        'stack': key['stack'],
        'pseudo_stack': key['pseudo_stack'],
        'stackHangMs': 0.0,
        'stackHangCount': 0.0,
    }))

    stack_table.key_to_index({'name': '(root)', 'lib': None, 'prefix': None})

    global tid
    global pid

    tid += 1
    pid += 1
    return {
        'name': name,
        'libs': libs,
        'funcTable': func_table,
        'stackTable': stack_table,
        'pseudoStackTable': pseudo_stack_table,
        'stackToPseudoStacksTable': stack_to_pseudo_stacks_table,
        'stringArray': strings_table,
        'processType': 'default',
        'tid': tid,
        'pid': pid,
        'time': 0.0,
        'dates': UniqueKeyedTable(lambda date: ({
            'date': date,
            'stackHangMs': GrowToFitList(),
            'stackHangCount': GrowToFitList()
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
        'pseudoStackTable': thread['pseudoStackTable'].struct_of_arrays(),
        'stackToPseudoStacksTable': thread['stackToPseudoStacksTable'].sorted_struct_of_arrays(lambda r: r['stack']),
        'dates': thread['dates'].get_items(),
        'stringArray': thread['stringArray'].get_items(),
    }

def process_into_profile(data):
    thread_table = UniqueKeyedTable(get_default_thread)
    preprocessed_thread_table = UniqueKeyedTable(lambda key: {})

    data = [
        (list(stack), pseudo, thread_name, build_date, hang_ms, hang_count)
        for stack, pseudo, thread_name, build_date, hang_ms, hang_count
        in data
    ]

    print "Preprocessing stacks for profile..."
    for row in data:
        stack, pseudo, thread_name, build_date, hang_ms, hang_count = row
        frame_to_prefix = preprocessed_thread_table.key_to_item(thread_name)

        last_frame = None
        for i in xrange(0,len(stack) - 1):
            frame_to_prefix[stack[i]] = stack[i + 1]

    for row in data:
        stack, pseudo, thread_name, build_date, hang_ms, hang_count = row

        if len(stack) == 25:
            frame_to_prefix = preprocessed_thread_table.key_to_item(thread_name)
            top_frame = stack[-1];

            # cap out at stacks of length == 25 + 75
            for i in xrange(0, 75):
                if top_frame in frame_to_prefix:
                    prefix = frame_to_prefix[top_frame]
                    stack.append(prefix)
                    top_frame = prefix
                else:
                    break

    print "Preprocessing all stacks for profile..."
    for row in data:
        stack, pseudo, thread_name, build_date, hang_ms, hang_count = row

        if len(stack) >= 100:
            continue

        thread = thread_table.key_to_item(thread_name)
        stack_table = thread['stackTable']
        pseudo_stack_table = thread['pseudoStackTable']
        stack_to_pseudo_stacks_table = thread['stackToPseudoStacksTable']
        dates = thread['dates']

        last_stack = 0
        last_stack_condensed = False
        last_lib_name = None
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

            condensed = False
            for condensed_name, patterns in stack_condense_groups:
                for pattern in patterns:
                    if re.search(pattern, func_name):
                        condensed = True
                        func_name = condensed_name
                        break
                if condensed:
                    break

            if condensed and last_stack_condensed and last_lib_name == lib_name:
                continue

            last_stack_condensed = condensed
            last_lib_name = lib_name
            last_stack = stack_table.key_to_index({
                'name': func_name,
                'lib': lib_name,
                'prefix': last_stack
            })

        date = dates.key_to_item(build_date)
        if date['stackHangMs'][last_stack] is None:
            date['stackHangMs'][last_stack] = 0.0
            date['stackHangCount'][last_stack] = 0

        date['stackHangMs'][last_stack] += hang_ms
        date['stackHangCount'][last_stack] += hang_count

        last_pseudo = 0
        for frame in reversed(pseudo):
            last_pseudo = pseudo_stack_table.key_to_index({
                'name': frame,
                'prefix': last_pseudo,
                'last_stack': last_stack
            })

        stack_to_pseudo_stack = stack_to_pseudo_stacks_table.key_to_item({
            'stack': last_stack,
            'pseudo_stack': last_pseudo
        })

        stack_to_pseudo_stack['stackHangMs'] += hang_ms
        stack_to_pseudo_stack['stackHangCount'] += hang_count

    print "Processing into final format..."
    return {
        'threads': [process_thread(t) for t in thread_table.get_items()],
    }
