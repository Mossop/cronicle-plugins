#! /usr/bin/env python

import time
from cronicle import CronicleError, CronicleEvent, CronicleAPI, CroniclePlugin

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
        self.log("Running event %s." % event.title)

        jobs = event.run()

        if len(jobs) > 1:
            raise CronicleError(2, "Event multiplexing is not supported.")

        job = jobs[0]
        while not job.complete:
            self.set_progress((pos + job.progress) / len(self.events))
            time.sleep(5)
            job.update_status()

        if job.code != 0:
            raise CronicleError(3, "Event %s failed." % event.title)

        self.set_progress(float(pos + 1) / len(self.events))
        self.set_perf(event.title, job.elapsed)
        self.log("Event %s completed successfully." % event.title)

if __name__ == "__main__":
    SequencePlugin()
