import inspect

class CronicleError(Exception):
    def __init__(self, *args):
        if len(args) == 1:
            self.init_with_exception(args[0])
        elif len(args) == 2:
            self.init_with_code(args[0], args[1])
        else:
            self.init_with_code(1, "CronicleError called with too many arguments.")

    def init_with_exception(self, e):
        trace = inspect.trace()[-1]
        self.code = -1
        self.description = "%s: %s (%s:%s)." % (type(e).__name__, str(e), trace[1], trace[2])

    def init_with_code(self, code, description):
        self.code = code
        self.description = description

    def __str__(self):
        return "%d: %s" % (self.code, self.description)
