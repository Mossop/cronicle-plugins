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

    @property
    def web_hook(self):
        if "web_hook" in self.event:
            return self.event["web_hook"]
        return None

    @property
    def multiplex(self):
        return self.event["multiplex"]

    def run(self):
        return self.api.run_event(self)
