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
                                         for lib, frame in stack)

def get_tracked_stats():
    return [AllHangs, DevtoolsHangs]
