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

    (match_prefix, 'mozilla::gfx', 'gfx'),
    (match_prefix, 'D2DDevice', 'os_gfx'),

    (match_prefix, 'mozJSComponentLoader::', 'js_component_load'),

    (match_exact, 'nsBaseFilePicker::AsyncShowFilePicker::Run()', 'filepicker'),

    (match_exact, 'nsBaseFilePicker::AsyncShowFilePicker::Run()', 'filepicker'),
]

def categorize_stack_with(stack, categories):
    for frame in reversed(stack):
        func_name = frame[0]
        for matches, pattern, category in categories:
            if matches(func_name, pattern):
                return category

def categorize_stack(stack):
    category_p1 = categorize_stack_with(stack, categories_p1)
    if category_p1 is not None:
        return category_p1
    category_p2 = categorize_stack_with(stack, categories_p2)
    if category_p2 is not None:
        return category_p2
    return "uncategorized"
