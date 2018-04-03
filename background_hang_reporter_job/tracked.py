class DevtoolsHangs(object):
    title = "Devtools Hangs"

    @staticmethod
    def matches_hang(hang):
        #pylint: disable=unused-variable
        stack, duration, thread, runnable, process, annotations, build_date, platform = hang
        return stack is not None and any(isinstance(frame, basestring) and "devtools/" in frame
                                         for frame, lib in stack)

class ActivityStreamHangs(object):
    title = "Activity Stream Hangs"

    @staticmethod
    def matches_hang(hang):
        #pylint: disable=unused-variable
        stack, duration, thread, runnable, process, annotations, build_date, platform = hang
        return stack is not None and any(isinstance(frame, basestring) and "activity-stream/" in frame
                                         for frame, lib in stack)

class PlacesHangs(object):
    title = "Places Hangs"

    @staticmethod
    def matches_hang(hang):
        #pylint: disable=unused-variable
        stack, duration, thread, runnable, process, annotations, build_date, platform = hang
        return stack is not None and any(isinstance(frame, basestring) and
                                         ("/places/" in frame or "/modules/Places" in frame)
                                         for frame, lib in stack)

class TelemetryHangs(object):
    title = "Telemetry Hangs"

    @staticmethod
    def matches_hang(hang):
        #pylint: disable=unused-variable
        stack, duration, thread, runnable, process, annotations, build_date, platform = hang
        return stack is not None and any(isinstance(frame, basestring) and
                                         ("/telemetry/" in frame or "/modules/Telemetry" in frame)
                                         for frame, lib in stack)

def get_tracked_stats():
    return [DevtoolsHangs, ActivityStreamHangs, PlacesHangs, TelemetryHangs]

def get_tracking_component(hang):
    for stat in get_tracked_stats():
        if stat.matches_hang(hang):
            return stat.title
    return "unknown_component"
