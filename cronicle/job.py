import sys
import time
from .error import CronicleError
from .utils import Lock, wait_for_callback

class CronicleJob:
    def __init__(self, api, event, hook_start_data):
        self.api = api
        self.event = event
        self.hook_start_data = hook_start_data
        self.hook_complete_data = None

        self.lock = Lock()
        self.callbacks = []

        self.update_status()

    def on_job_complete(self, data):
        if data["id"] != self.id:
            raise CronicleError(100, "Received a complete notification for the wrong job.")
        self.update_status(3)
        with self.lock:
            self.hook_complete_data = data
            callbacks = self.callbacks
            self.callbacks = None
        for callback in callbacks:
            callback(self)

    def update_status(self, retries = 0):
        while True:
            try:
                job = self.api.call_api("get_job_status", { "id": self.id })["job"]
                break
            except Exception as e:
                if retries == 0:
                    raise
                retries -= 1
                time.sleep(2)

        with self.lock:
            self.job = job

    def on_complete(self, callback):
        if self.is_complete:
            callback(self)
        with self.lock:
            self.callbacks.append(callback)

    def wait_for_complete(self):
        wait_for_callback(self.on_complete)

    def safe_get(self, property, default = None):
        with self.lock:
            if property in self.job:
                return self.job[property]
        return default

    @property
    def id(self):
        return self.hook_start_data["id"]

    @property
    def is_complete(self):
        return self.safe_get("complete", 0) == 1

    @property
    def is_failed(self):
        return self.safe_get("code", 0) != 0

    @property
    def progress(self):
        if self.is_complete:
            return 1.0
        return self.safe_get("progress", 0.0)

    @property
    def elapsed(self):
        return self.safe_get("elapsed")

    @property
    def code(self):
        return self.safe_get("code")

    @property
    def description(self):
        return self.safe_get("description")

    @property
    def details_url(self):
        return self.hook_start_data["job_details_url"]

class CronicleQueuedJob:
    def __init__(self, api, event):
        self.api = api
        self.event = event
        self.lock = Lock()
        self.job = None
        self.callbacks = []
        self.started = False

    def on_job_start(self, job):
        with self.lock:
            self.job = job
            self.started = True
            callbacks = self.callbacks
            self.callbacks = None
        for callback in callbacks:
            callback(job)

    def on_job_launch_failure(self):
        with self.lock:
            self.started = True
            callbacks = self.callbacks
            self.callbacks = None
        for callback in callbacks:
            callback(None)

    def on_job_started(self, callback):
        with self.lock:
            if self.started:
                callback(self.job)
                return
            self.callbacks.append(callback)

    def wait_for_job(self):
        job = wait_for_callback(self.on_job_started)

        if job is None:
            raise CronicleError(101, "Event %s failed to start." % self.event.title)
        return job
