#! /usr/bin/env python

import time
from cronicle import CronicleError, CroniclePlugin, CronicleAPI
from cronicle.utils import Flag

class SequencePlugin(CroniclePlugin):
    def execute(self, params):
        self.api = CronicleAPI(params["api_host"], params["api_key"])
        self.events = []

        titles = params["events"].strip().split("\n")
        for title in titles:
            event = self.api.get_event(title = title)
            if event.enabled:
                self.events.append(event)
            else:
                raise CronicleError(1, "Event %s is not enabled." % title)

        for i, event in enumerate(self.events):
            self.run_event(event, i)

    def run_event(self, event, pos):
        self.log("Starting event '%s'." % event.title)

        queue = event.run()
        job = queue.wait_for_job()
        self.log("Job '%s' is running: %s" % (job.id, job.details_url))

        done = Flag()
        def complete(job):
            done.set()
        job.on_complete(complete)

        while not done:
            self.set_progress((pos + job.progress) / len(self.events))

            count = 0
            while count < 25 and not done:
                time.sleep(0.2)

            if not done:
                job.update_status()
                if job.complete:
                    done.set()

        if job.is_failed:
            result = "%d: %s" % (job.code, job.description)
            raise CronicleError(3, "Event '%s' failed (%s)" % (event.title, result))

        self.set_progress(float(pos + 1) / len(self.events))
        self.set_perf(event.title, job.elapsed)
        self.log("Event '%s' completed successfully." % event.title)

if __name__ == "__main__":
    SequencePlugin()
