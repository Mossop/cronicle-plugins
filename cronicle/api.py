import json
from urlparse import urlparse, urljoin
from httplib import HTTPConnection, HTTPException
from .error import CronicleError
from .hookmanager import HookManager
from .event import CronicleEvent

class CronicleAPI:
    def __init__(self, host, key):
        self.key = key
        self.hook_manager = None

        self.url = urljoin(host, "/api/")
        parts = urlparse(self.url)
        if parts[0] != "http":
            raise CronicleError(100, "Unsupported scheme for API: %s." % host)

        self.host = parts[1]

    def run_event(self, event):
        if self.hook_manager is None:
            self.hook_manager = HookManager(self)
        return self.hook_manager.run_event(event)

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
