class AllHangs(object):
    title = "All Hangs"

    @staticmethod
    def matches_hang(_):
        return True

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

def get_tracked_stats():
    return [AllHangs, DevtoolsHangs, ActivityStreamHangs]
