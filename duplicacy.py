#! /usr/bin/env python

import re
import sys
import json
import subprocess

log_re = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} (?P<level>\S+) (?P<type>\S+) (?P<message>.+)$")
percent_re = re.compile(r"(?P<percent>\d+\.\d+)%")
progress_re = re.compile(r"\((?P<done>\d+)/(?P<total>\d+)\)")

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
    def annotate_line(self, level, type, message):
        if type == "UPLOAD_PROGRESS":
            match = percent_re.search(message)
            if match:
                progress = float(match.group("percent")) / 100
                print_json({ "progress": progress })

class CopyParser(LogParser):
    def annotate_line(self, level, type, message):
        if type == "SNAPSHOT_COPY":
            match = progress_re.search(message)
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
    args = [duplicacy, "-log", command]
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

