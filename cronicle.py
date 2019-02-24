import sys
import json
import inspect
from urlparse import urljoin, urlparse
from httplib import HTTPConnection, HTTPException

def print_json(data):
    print(json.dumps(data))

class CronicleError(Exception):
    def __init__(self, code, description):
        self.code = code
        self.description = description

    def __str__(self):
        return "%d: %s" % (self.code, self.description)

class CroniclePlugin:
    def __init__(self, stdin = sys.stdin, stdout = sys.stdout):
        self.stdin = stdin
        self.stdout = stdout
        self.perf = {}

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

        except CronicleError as e:
            result["code"] = e.code
            result["description"] = e.description
        except Exception as e:
            trace = inspect.trace()[-1]
            result["code"] = -1
            result["description"] = "%s: %s (%s:%s)." % (type(e).__name__, str(e), trace[1], trace[2])

        self.log(json.dumps(result))

    def log(self, line):
        self.stdout.write("%s\n" % line)
        self.stdout.flush()

    def log_json(self, data):
        self.log(json.dumps(data))

    def set_progress(self, progress):
        self.log_json({ "progress": progress })

    def set_perf(self, name, time):
        self.perf[name] = time

class CronicleJob:
    def __init__(self, api, id):
        self.id = id
        self.api = api
        self.update_status()

    def update_status(self):
        self.job = self.api.call_api("get_job_status", { "id": self.id })["job"]

    @property
    def complete(self):
        return "complete" in self.job and self.job["complete"] == 1

    @property
    def progress(self):
        if "progress" in self.job:
            return self.job["progress"]
        if self.complete:
            return 1.0
        return 0.0

    @property
    def elapsed(self):
        return self.job["elapsed"]

    @property
    def code(self):
        return self.job["code"]

    @property
    def description(self):
        return self.job["description"]

class CronicleEvent:
    def __init__(self, api, event):
        self.api = api
        self.event = event

    @property
    def id(self):
        return self.event["id"]

    @property
    def title(self):
        return self.event["title"]

    @property
    def enabled(self):
        return self.event["enabled"] != 0

    def run(self):
        result = self.api.call_api("run_event", { "id": self.id })
        return [CronicleJob(self.api, id) for id in result["ids"]]

class CronicleAPI:
    def __init__(self, host, key):
        self.key = key

        self.url = urljoin(host, "/api/")
        parts = urlparse(self.url)
        if parts[0] != "http":
            raise CronicleError(100, "Unsupported scheme for API: %s." % host)

        self.host = parts[1]

    def call_api(self, name, params):
        url = urljoin(self.url, "app/%s/v1" % name)
        params["api_key"] = self.key
        headers = {
            "Content-Type": "application/json",
        }

        try:
            connection = HTTPConnection(self.host)
            connection.request("POST", url, json.dumps(params), headers)

            response = connection.getresponse()
        except HTTPException as e:
            raise CronicleError(100, "API call failed: %s." % str(e))

        if response.status <200 or response.status >=300:
            raise CronicleError(100, "API call failed: %d %s." % (response.status, response.reason))

        try:
            result = json.loads(response.read())
        except:
            raise CronicleError(100, "API call returned unparsable data.")

        if result["code"] != 0:
            raise CronicleError(100, "API call failed with result: '(%s) %s'." % (result["code"], result["description"]))

        return result

    def get_event(self, id = None, title = None):
        params = {}
        if id is not None and len(id) > 0:
            params["id"] = id
        elif title is not None and len(title) > 0:
            params["title"] = title
        else:
            raise CronicleError(100, "Attempt to retrieve an event with no id or title.")

        event = self.call_api("get_event", params)["event"]
        return CronicleEvent(self, event)
