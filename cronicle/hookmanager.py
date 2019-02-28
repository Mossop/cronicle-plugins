import re
import sys
import json
import time
import threading
from uuid import uuid4
from urlparse import urlparse
from httplib import HTTPConnection
from BaseHTTPServer import HTTPServer, BaseHTTPRequestHandler
from utils import Lock, DataEvent
from .error import CronicleError
from .job import CronicleQueuedJob, CronicleJob

class HookRequestHandler(BaseHTTPRequestHandler):
    def log_request(self, code='-', size='-'):
        pass

    def do_POST(self):
        try:
            try:
                data = json.loads(self.rfile.readline())
            except:
                self.send_response(400)
                return

            self.send_response(200)
            self.server.manager.handle_request(self.path, data)
        except Exception as e:
            if not isinstance(e, CronicleError):
                e = CronicleError(e)
            sys.stderr.write("%s\n" % str(e))

class HookServer(HTTPServer):
    def __init__(self, manager, address):
        HTTPServer.__init__(self, (address, 0), HookRequestHandler)
        self.manager = manager

        self.thread = threading.Thread(target=self)
        self.thread.daemon = True
        self.thread.start()

    def __call__(self):
        self.serve_forever()

class Hook:
    def __init__(self, api, event, queued_job):
        self.api = api
        self.event = event
        self.queued_job = queued_job
        self.job = None

    def on_hook_data(self, data):
        next_hook = self.event.web_hook
        if next_hook is not None:
            try:
                host = urlparse(next_hook)[1]
                connection = HTTPConnection(host)
                connection.request("POST", next_hook, json.dumps(data))
            except:
                pass

        if data["action"] == "job_launch_failure":
            self.on_job_launch_failure()
            return False
        elif data["action"] == "job_start":
            self.on_job_start(data)
            return True
        elif data["action"] == "job_complete":
            self.on_job_complete(data)
            return False
        else:
            raise CronicleError(100, "Saw unknown job action: %s." % data["action"])

    def on_job_start(self, data):
        if self.job is not None:
            raise CronicleError(100, "Saw job_start for a job that already started.")
        self.job = CronicleJob(self.api, self.event, data)
        self.queued_job.on_job_start(self.job)

    def on_job_launch_failure(self):
        if self.job is not None:
            raise CronicleError(100, "Saw job_launch_failure for a job that already started.")
        self.queued_job.on_job_failure()

    def on_job_complete(self, data):
        if self.job is None:
            raise CronicleError(100, "Saw job_complete for a job that never started.")
        self.job.on_job_complete(data)

class HookManager:
    def __init__(self, api):
        self.api = api
        self.address = "127.0.0.1"
        self.server = HookServer(self, self.address)
        self.hooks = {}
        self.lock = Lock()

    def create_hook_id(self):
        id = str(uuid4())

        if id not in self.hooks:
            return id
        return self.create_hook_id()

    def handle_request(self, path, data):
        id = path[1:]
        with self.lock:
            if id not in self.hooks:
                raise CronicleError(100, "Saw a request for an unknown web hook.")
            hook = self.hooks[id]

        if not hook.on_hook_data(data):
            with self.lock:
                del self.hooks[id]

    def run_event(self, event):
        if event.multiplex:
            raise CronicleError(100, "API does not support running multiplexed events.")

        queued_job = CronicleQueuedJob(self.api, event)

        with self.lock:
            hook_id = self.create_hook_id()
            self.hooks[hook_id] = Hook(self.api, event, queued_job)

        new_hook_url = "http://%s:%s/%s" % (self.address, self.server.server_port, hook_id)
        self.api.call_api("run_event", { "id": event.id, "web_hook": new_hook_url })

        return queued_job
