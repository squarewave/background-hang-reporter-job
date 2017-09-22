import re
from sets import Set

def match_exact(symbol, pattern):
    return symbol == pattern

def match_prefix(symbol, pattern):
    return symbol.startswith(pattern)

def match_substring(symbol, pattern):
    return pattern in symbol

def match_stem(symbol, pattern):
    return symbol == pattern or symbol.startswith(pattern + '(')

categories_p1 = [
    (match_exact, '(content script)', 'content_script'),
]

categories_p2 = [
    (match_stem, 'mozilla::ipc::MessageChannel::WaitForSyncNotify', 'sync_ipc'),
    (match_stem, 'mozilla::ipc::MessageChannel::WaitForInterruptNotify', 'sync_ipc'),
    (match_prefix, 'mozilla::places::', 'places'),
    (match_prefix, 'mozilla::plugins::', 'plugins'),

    (match_stem, 'js::RunScript', 'script'),
    (match_stem, 'js::Nursery::collect', 'GC'),
    (match_stem, 'js::GCRuntime::collect', 'GC'),
    (match_stem, 'nsJSContext::GarbageCollectNow', 'GC'),
    (match_prefix, 'mozilla::RestyleManager::', 'restyle'),
    (match_substring, 'RestyleManager', 'restyle'),
    (match_stem, 'mozilla::PresShell::ProcessReflowCommands', 'layout'),
    (match_prefix, 'nsCSSFrameConstructor::', 'frameconstruction'),
    (match_stem, 'mozilla::PresShell::DoReflow', 'layout'),
    (match_substring, '::compileScript(', 'script'),

    (match_prefix, 'nsCycleCollector', 'CC'),
    (match_prefix, 'nsPurpleBuffer', 'CC'),
    (match_prefix, 'nsRefreshDriver::IsWaitingForPaint', 'paint'), # arguable, I suppose
    (match_stem, 'mozilla::PresShell::Paint', 'paint'),
    (match_stem, 'mozilla::PresShell::DoUpdateApproximateFrameVisibility', 'layout'), # could just as well be paint
    (match_substring, 'mozilla::net::', 'network'),
    (match_stem, 'nsInputStreamReadyEvent::Run', 'network'),

    # (match_stem, 'NS_ProcessNextEvent', 'eventloop'),
    (match_stem, 'nsJSUtil::EvaluateString', 'script'),
    (match_prefix, 'js::frontend::Parser', 'script.parse'),
    (match_prefix, 'js::jit::IonCompile', 'script.compile.ion'),
    (match_prefix, 'js::jit::BaselineCompiler::compile', 'script.compile.baseline'),

    (match_prefix, 'CompositorBridgeParent::Composite', 'paint'),
    (match_prefix, 'mozilla::layers::PLayerTransactionParent::Read(', 'messageread'),

    (match_prefix, 'mozilla::dom::', 'dom'),
    (match_prefix, 'nsDOMCSSDeclaration::', 'restyle'),
    (match_prefix, 'nsHTMLDNS', 'network'),
    (match_substring, 'IC::update(', 'script.icupdate'),
    (match_prefix, 'js::jit::CodeGenerator::link(', 'script.link'),

    (match_exact, 'base::WaitableEvent::Wait()', 'idle'),

    # Can't do this until we come up with a way of labeling ion/baseline.
    (match_prefix, 'Interpret(', 'script.execute.interpreter'),
]

def to_struct_of_arrays(a):
    if len(a) == 0:
        raise Exception('Need at least one item in array for this to work.')

    result = {k:[e[k] for e in a] for k in a[0].keys()}
    result['length'] = len(a)
    return result

class UniqueKeyedTable(object):
    def __init__(self, get_default_from_key, key_names=()):
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

    def inner_struct_of_arrays(self, items):
        if len(items) == 0:
            raise Exception('Need at least one item in array for this to work.')

        result = {}
        num_keys = len(self.key_names)
        for i in xrange(0, num_keys):
            result[self.key_names[i]] = [x[i] for x in items]

        result['length'] = len(items)
        return result

    def struct_of_arrays(self):
        return self.inner_struct_of_arrays(self.items)

    def sorted_struct_of_arrays(self, key):
        return self.inner_struct_of_arrays(sorted(self.items, key=key))

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

def get_default_lib(name):
    return ({
        'name': re.sub(r'\.pdb$', '', name),
        'offset': 0,
        'path': "",
        'debugName': name,
        'debugPath': name,
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
        key[2],
        strings_table.key_to_index(key[3]),
    ), ('stack', 'runnable', 'userInteracting', 'platform'))

    stack_table.key_to_index(('(root)', None, None))

    prune_stack_cache = UniqueKeyedTable(lambda key: [0.0])
    prune_stack_cache.key_to_index(('(root)', None, None))

    return {
        'name': name,
        'libs': libs,
        'funcTable': func_table,
        'stackTable': stack_table,
        'pruneStackCache': prune_stack_cache,
        'sampleTable': sample_table,
        'stringArray': strings_table,
        'processType': 'tab' if name == 'Gecko_Child' or name == 'Gecko_Child_ForcePaint' else 'default',
        'time': 0.0,
        'dates': UniqueKeyedTable(lambda date: ({
            'date': date,
            'sampleHangMs': GrowToFitList(),
            'sampleHangCount': GrowToFitList()
        }), ('date', 'sampleHangMs', 'sampleHangCount')),
    }

def sample_categorizer(categories, stack_table, func_table, string_array):
    func_name_to_category_cache = {}

    def function_name_to_category(name):
        if name in func_name_to_category_cache:
            return func_name_to_category_cache[name]

        for matches, pattern, category in categories:
            if matches(name, pattern):
                func_name_to_category_cache[name] = category
                return category

        func_name_to_category_cache[name] = False
        return False

    stack_category_cache = {}

    def compute_category(stack_index):
        while True:
            if stack_index in stack_category_cache:
                return stack_category_cache[stack_index]
            else:
                if not stack_index:
                    stack_category_cache[stack_index] = None
                    return None
                else:
                    func_index = stack_table['func'][stack_index]
                    name = string_array.index_to_item(func_table['name'][func_index])
                    category = function_name_to_category(name)

                if category != False:
                    stack_category_cache[stack_index] = category
                    return category

                stack_index = stack_table['prefix'][stack_index]

    return compute_category

def process_thread(thread):
    string_array = thread['stringArray']
    func_table = thread['funcTable'].struct_of_arrays()
    stack_table = thread['stackTable'].struct_of_arrays()
    categorizer_p1 = sample_categorizer(categories_p1, stack_table, func_table, string_array)
    categorizer_p2 = sample_categorizer(categories_p2, stack_table, func_table, string_array)

    sample_table = thread['sampleTable'].struct_of_arrays()
    sample_table['category'] = []
    for s in sample_table['stack']:
        category_string = categorizer_p1(s)
        if category_string is None:
            category_string = categorizer_p2(s)
            if category_string is None:
                sample_table['category'].append(None)
            else:
                sample_table['category'].append(string_array.key_to_index(category_string))
        else:
            sample_table['category'].append(string_array.key_to_index(category_string))

    return {
        'name': thread['name'],
        'processType': thread['processType'],
        'libs': thread['libs'].get_items(),
        'funcTable': func_table,
        'stackTable': stack_table,
        'sampleTable': sample_table,
        'stringArray': string_array.get_items(),
        'dates': thread['dates'].get_items(),
    }

def reconstruct_stack(string_array, func_table, stack_table, lib_table, stack_index):
    result = []
    while stack_index != 0:
        func_index = stack_table['func'][stack_index]
        prefix = stack_table['prefix'][stack_index]
        func_name = string_array[func_table['name'][func_index]]
        lib_name = lib_table[func_table['lib'][func_index]]['debugName']
        result.append((func_name, lib_name))
        stack_index = prefix
    return result[::-1]

def merge_number_dicts(a, b):
    keys = Set(a.keys() + b.keys())
    return {k: a.get(k, 0.) + b.get(k, 0.) for k in keys}

class ProfileProcessor(object):
    def __init__(self, config):
        self.config = config
        self.thread_table = UniqueKeyedTable(get_default_thread)
        self.usage_hours_by_date = {}

    def debugDump(self, dump_str):
        if self.config['print_debug_info']:
            print dump_str

    def ingest_processed_profile(self, profile):
        for existing_thread in self.thread_table.get_items():
            prune_stack_cache = UniqueKeyedTable(lambda key: [0.0])
            prune_stack_cache.key_to_index(('(root)', None, None))
            existing_thread['pruneStackCache'] = prune_stack_cache

        threads = profile['threads']
        for other in threads:
            other_samples = other['sampleTable']
            other_dates = other['dates']

            for date in other_dates:
                build_date = date['date']
                for i in xrange(0, len(date['sampleHangCount'])):
                    stack_index = other_samples['stack'][i]
                    stack = reconstruct_stack(other['stringArray'],
                                              other['funcTable'],
                                              other['stackTable'],
                                              other['libs'],
                                              stack_index)
                    self.pre_ingest_row((stack,
                                         other['stringArray'][other_samples['runnable'][i]],
                                         other['name'],
                                         build_date,
                                         other_samples['userInteracting'][i],
                                         other['stringArray'][other_samples['platform'][i]],
                                         date['sampleHangMs'][i],
                                         date['sampleHangCount'][i]))

            for date in other_dates:
                build_date = date['date']
                for i in xrange(0, len(date['sampleHangCount'])):
                    stack_index = other_samples['stack'][i]
                    stack = reconstruct_stack(other['stringArray'],
                                              other['funcTable'],
                                              other['stackTable'],
                                              other['libs'],
                                              stack_index)
                    self.ingest_row((stack,
                                     other['stringArray'][other_samples['runnable'][i]],
                                     other['name'],
                                     build_date,
                                     other_samples['userInteracting'][i],
                                     other['stringArray'][other_samples['platform'][i]],
                                     date['sampleHangMs'][i],
                                     date['sampleHangCount'][i]))
        self.usage_hours_by_date = merge_number_dicts(self.usage_hours_by_date, profile.get('usageHoursByDate', {}))

    def pre_ingest_row(self, row):
        #pylint: disable=unused-variable
        stack, runnable_name, thread_name, build_date, pending_input, platform, hang_ms, hang_count = row

        thread = self.thread_table.key_to_item(thread_name)
        prune_stack_cache = thread['pruneStackCache']
        root_stack = prune_stack_cache.key_to_item(('(root)', None, None))
        root_stack[0] += hang_ms

        last_stack = 0
        for (func_name, lib_name) in stack:
            last_stack = prune_stack_cache.key_to_index((func_name, lib_name, last_stack))
            cache_item = prune_stack_cache.index_to_item(last_stack)
            cache_item[0] += hang_ms

    def ingest_row(self, row):
        #pylint: disable=unused-variable
        stack, runnable_name, thread_name, build_date, pending_input, platform, hang_ms, hang_count = row

        thread = self.thread_table.key_to_item(thread_name)
        stack_table = thread['stackTable']
        sample_table = thread['sampleTable']
        dates = thread['dates']
        prune_stack_cache = thread['pruneStackCache']
        root_stack = prune_stack_cache.key_to_item(('(root)', None, None))

        last_stack = 0
        last_cache_item_index = 0
        last_lib_name = None
        for (func_name, lib_name) in stack:
            cache_item_index = prune_stack_cache.key_to_index((func_name, lib_name, last_cache_item_index))
            cache_item = prune_stack_cache.index_to_item(cache_item_index)
            parent_cache_item = prune_stack_cache.index_to_item(last_cache_item_index)
            if cache_item[0] / parent_cache_item[0] > self.config['stack_acceptance_threshold']:
                last_lib_name = lib_name
                last_stack = stack_table.key_to_index((func_name, lib_name, last_stack))
                last_cache_item_index = cache_item_index
            else:
                # If we're below the acceptance threshold, just lump it under (other) below
                # its parent.
                last_lib_name = lib_name
                last_stack = stack_table.key_to_index(('(other)', lib_name, last_stack))
                last_cache_item_index = cache_item_index
                break

        sample_index = sample_table.key_to_index((last_stack, runnable_name, pending_input, platform))

        date = dates.key_to_item(build_date)
        if date['sampleHangMs'][sample_index] is None:
            date['sampleHangMs'][sample_index] = 0.0
            date['sampleHangCount'][sample_index] = 0

        date['sampleHangMs'][sample_index] += hang_ms
        date['sampleHangCount'][sample_index] += hang_count

    def ingest(self, data, usage_hours_by_date):
        print "{} unfiltered samples in data".format(len(data))
        data = [
            x
            for x in data
            # x[6] should be hang_ms
            if x[6] > 0.0
        ]
        print "{} filtered samples in data".format(len(data))

        print "Preprocessing stacks for prune cache..."
        for row in data:
            self.pre_ingest_row(row)

        print "Processing stacks..."
        for row in data:
            self.ingest_row(row)

        self.usage_hours_by_date = merge_number_dicts(self.usage_hours_by_date, usage_hours_by_date)

    def process_into_profile(self):
        print "Processing into final format..."
        return {
            'threads': [process_thread(t) for t in self.thread_table.get_items()],
            'usageHoursByDate': self.usage_hours_by_date,
            'uuid': self.config['uuid'],
        }
