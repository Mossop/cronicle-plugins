import sys
import json
import inspect
import subprocess
from .error import CronicleError

class ProcessLogParser:
    def parse_line(self, line):
        pass

    def process_complete(self, code):
        if code != 0:
            raise CronicleError(code, "Process exited with exit code %d." % code)

class TextParser(ProcessLogParser):
    def __init__(self):
        self.lines = []

    def parse_line(self, line):
        self.lines.append(line)

    def process_complete(self, code):
        if (code != 0):
            return ProcessLogParser.process_complete(self, code)
        return self.lines

class JsonParser(TextParser):
    def process_complete(self, code):
        text = "\n".join(TextParser.process_complete(self, code))
        try:
            return json.loads(text)
        except:
            raise CronicleError(2, "Process returned invalid json.")

class CroniclePlugin:
    def __init__(self, start = True, stdin = sys.stdin, stdout = sys.stdout):
        self.stdin = stdin
        self.stdout = stdout
        self.perf = {}
        self.last_progress = 0.0

        if start:
            self.start()

    def execute(self, params):
        pass

    def start(self):
        result = { "complete": 1 }

        try:
            try:
                self.arguments = json.load(self.stdin)
            except:
                raise CronicleError(1, "Invalid input arguments")

            self.execute(self.arguments["params"])

            if len(self.perf) > 0:
                self.log_json({ "perf": self.perf })

        except Exception as e:
            if not isinstance(e, CronicleError):
                e = CronicleError(e)
            result["code"] = e.code
            result["description"] = e.description

        self.log(json.dumps(result))

    def exec_process(self, args, parser, cwd = None):
        process = subprocess.Popen(args,
                                   cwd=cwd,
                                   stdin=None,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT)

        for line in process.stdout:
            parser.parse_line(line.strip())

        code = process.wait()
        return parser.process_complete(code)

    def log(self, line):
        self.stdout.write("%s\n" % line)
        self.stdout.flush()

    def log_json(self, data):
        self.log(json.dumps(data))

    def set_progress(self, progress):
        if progress != self.last_progress:
            self.log_json({ "progress": progress })

    def set_perf(self, name, time):
        self.perf[name] = time

    def log_table(self, title, headers, rows, caption = None):
        stats = {
            "table": {
                "title": title,
                "header": headers,
                "rows": rows,
            },
        }

        if caption:
            stats["table"]["caption"] = caption

        self.log_json(stats)
