#! /usr/bin/env python

import re
import sys
import json
import subprocess

log_re = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} (?P<level>\S+) (?P<type>\S+) (?P<message>.+)$")

def print_json(data):
    print(json.dumps(data))

class LogParser:
    def __init__(self):
        pass

    def parse_line(self, line):
        match = log_re.match(line)
        if match:
            return { "level": match.group("level"), "type": match.group("type"), "message": match.group("message") }
        else:
            return None

    def annotate_line(self, level, type, message):
        pass

    def log_line(self, line):
        line_data = self.parse_line(line)
        if line_data:
            if line_data["level"] != "DEBUG" and line_data["level"] != "TRACE":
                print(line_data["message"])
            self.annotate_line(line_data["level"], line_data["type"], line_data["message"])
        else:
            print(line)

    def complete(self, code):
        response = { "complete": "1" }
        if code != 0:
            response["code"] = code
            if code in error_codes:
                response["description"] = error_codes[code]
            else:
                response["description"] = "Unknown error."
        print_json(response)

class BackupParser(LogParser):
    def __init__(self):
        LogParser.__init__(self)
        self.percent_re = re.compile(r"(?P<percent>\d+\.\d+)%")

        self.stats = {}

        self.stats_names = {
            "new_files": "New files",
            "changed_files": "Changed files",
            "unchanged_files": "Unchanged files",
            "removed_files": "Removed files",
            "file_chunks": "File chunks",
            "metadata_chunks": "Metadata chunks",
        }

        self.stats_re = {
            "new_files": re.compile(r"New files: (?P<count>\d+) total, (?P<size>\w+) bytes"),
            "changed_files": re.compile(r"Changed files: (?P<count>\d+) total, (?P<size>\w+) bytes"),
            "unchanged_files": re.compile(r"Unchanged files: (?P<count>\d+) total, (?P<size>\w+) bytes"),
            "removed_files": re.compile(r"Removed files: (?P<count>\d+) total, (?P<size>\w+) bytes"),
            "file_chunks": re.compile(r"File chunks: \d+ total, \w+ bytes; (?P<count>\d+) new, \w+ bytes, (?P<size>\w+) bytes uploaded"),
            "metadata_chunks": re.compile(r"Metadata chunks: \d+ total, \w+ bytes; (?P<count>\d+) new, \w+ bytes, (?P<size>\w+) bytes uploaded"),
        }

    def annotate_line(self, level, type, message):
        if type == "UPLOAD_PROGRESS":
            match = self.percent_re.search(message)
            if match:
                progress = float(match.group("percent")) / 100
                print_json({ "progress": progress })
        elif type == "BACKUP_STATS":
            for key in self.stats_re:
                match = self.stats_re[key].search(message)
                if match:
                    self.stats[key] = { "count": match.group("count"), "size": match.group("size") }

    def complete(self, code):
        if len(self.stats) > 0:
            stats = {
                "table": {
                    "title": "Backup statistics",
                    "header": ["Type", "Count", "Size"],
                    "rows": [],
                    "caption": "Various useful backup statistics.",
                },
            }

            for stat in ["new_files", "changed_files", "unchanged_files", "removed_files", "file_chunks", "metadata_chunks"]:
                if stat in self.stats:
                    stats["table"]["rows"].append([self.stats_names[stat], self.stats[stat]["count"], self.stats[stat]["size"]])

            print_json(stats)

        LogParser.complete(self, code)

class CopyParser(LogParser):
    def __init__(self):
        LogParser.__init__(self)
        self.progress_re = re.compile(r"\((?P<done>\d+)/(?P<total>\d+)\)")

    def annotate_line(self, level, type, message):
        if type == "SNAPSHOT_COPY":
            match = self.progress_re.search(message)
            if match:
                progress = float(match.group("done")) / float(match.group("total"))
                print_json({ "progress": progress })

command_parsers = {
    "backup": BackupParser,
    "copy": CopyParser,
}

error_codes = {
    "1": "Interrupted by user.",
    "2": "Invalid arguments.",
    "3": "Invalid command.",
    "100": "Runtime error.",
    "101": "Runtime error.",
}

def build_args(duplicacy, command, arguments):
    args = [duplicacy, "-debug", "-log", command]
    if command == "backup" or command == "check":
        args.append("-stats")
    if len(arguments) > 0:
        args.extend(arguments.split())
    return args

if __name__ == "__main__":
    input = sys.stdin.readline()
    data = json.loads(input)
    params = data["params"]

    args = build_args(params["duplicacy"], params["command"], params["arguments"])

    if params["command"] not in command_parsers:
        parser = LogParser()
    else:
        parser = command_parsers[params["command"]]()

    process = subprocess.Popen(args,
                               cwd=params["repository"],
                               stdin=None,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.STDOUT)

    for line in process.stdout:
        parser.log_line(line.strip())

    code = process.wait()

    parser.complete(code)

