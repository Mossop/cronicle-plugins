#! /usr/bin/env python

import re
from cronicle import CronicleError, CroniclePlugin, JsonParser

BITS_PER_GB = 8 * 1024 * 1024 * 1024

class SpeedTestPlugin(CroniclePlugin):
    def execute(self, params):
        args = [params["speedtest-cli"], "--json"]
        if not params["upload"]:
            args.append("--no-upload")
        if not params["download"]:
            args.append("--no-download")
        json = self.exec_process(args, JsonParser())

        if "upload" in json:
            self.set_perf("upload", BITS_PER_GB / json["upload"])

        if "download" in json:
            self.set_perf("download", BITS_PER_GB / json["download"])

if __name__ == "__main__":
    SpeedTestPlugin()
