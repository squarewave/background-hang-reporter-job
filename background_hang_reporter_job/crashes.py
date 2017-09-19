from moztelemetry import get_pings_properties
from moztelemetry.dataset import Dataset

def map_frame(frame, modules):
    if 'ip' not in frame:
        return None
    if 'module_index' not in frame:
        return None
    offset = int(frame['ip'], 16)
    module_index = frame['module_index']
    if module_index is None:
        return None
    if modules is not None and module_index >= 0 and module_index < len(modules):
        if 'base_addr' in modules[module_index]:
            offset -= int(modules[module_index]['base_addr'], 16)
    return [module_index, hex(offset)]

def get_payload_hangs(ping):
    if ping["payload/stackTraces/threads"] is None:
        return []
    if ping["payload/stackTraces/crash_info/crashing_thread"] is None:
        return []
    crash_thread = ping["payload/stackTraces/threads"][ping["payload/stackTraces/crash_info/crashing_thread"]]
    frames = crash_thread['frames']
    modules = get_payload_modules(ping)

    stack = [
        map_frame(f, ping["payload/stackTraces/modules"])
        for f in reversed(frames)
    ]
    return [{
        'stack': [f for f in stack if f is not None],
        'duration': 1,
        'thread': ping['payload/processType'],
        'runnableName': 'dummy_runnable',
        'process': ping['payload/processType'],
        'annotations': {},
    }]

def get_payload_modules(ping):
    crash_modules = ping["payload/stackTraces/modules"]
    if crash_modules is None:
        return []
    return [[m['debug_file'], m['debug_id']] for m in crash_modules]

def map_to_hang_format(ping):
    return {
        "environment/system/os/name": ping["environment/system/os/name"],
        "environment/system/os/version": ping["environment/system/os/version"],
        "application/architecture": ping["application/architecture"],
        "application/buildId": ping["application/buildId"],
        "payload/stackTraces/crash_info/crashing_thread": ping["payload/stackTraces/crash_info/crashing_thread"],
        "payload/stackTraces/threads": ping["payload/stackTraces/threads"],
        "payload/stackTraces/modules": ping["payload/stackTraces/modules"],
        "payload/hangs": get_payload_hangs(ping),
        "payload/modules": get_payload_modules(ping),
        "payload/timeSinceLastPing": 86400,
    }

def get_data(sc, config, date):
    date_str = date.strftime("%Y%m%d")

    pings = (Dataset.from_source("telemetry")
             .where(docType='crash')
             .where(submissionDate=lambda b: b.startswith(date_str))
             .where(appUpdateChannel=config['channel'])
             .records(sc, sample=config['sample_size']))

    properties = ["environment/system/os/name",
                  "environment/system/os/version",
                  "application/architecture",
                  "application/buildId",
                  "payload/processType",
                  "payload/stackTraces/crash_info/crashing_thread",
                  "payload/stackTraces/threads",
                  "payload/stackTraces/modules",]

    try:
        pings_props = get_pings_properties(pings, properties, with_processes=True)
    except ValueError:
        return None

    return pings_props.map(map_to_hang_format)
