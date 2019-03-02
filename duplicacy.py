#! /usr/bin/env python

import re
from cronicle import CronicleError, CroniclePlugin
from cronicle.plugin import ProcessLogParser

log_re = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} (?P<level>\S+) (?P<type>\S+) (?P<message>.+)$")

class DuplicacyLogParser(ProcessLogParser):
    def __init__(self, plugin):
        self.plugin = plugin

    def process_complete(self, code):
        if code in error_codes:
            raise CronicleError(code, error_codes[code])
        return ProcessLogParser.process_complete(self, code)

    def parse_duplicacy_line(self, line):
        match = log_re.match(line)
        if match:
            return { "level": match.group("level"), "type": match.group("type"), "message": match.group("message") }
        else:
            return None

    def log_line(self, level, type, message):
        if level != "DEBUG" and level != "TRACE":
            self.plugin.log(message)

    def annotate_line(self, level, type, message):
        pass

    def parse_line(self, line):
        line_data = self.parse_duplicacy_line(line)
        if line_data:
            self.log_line(line_data["level"], line_data["type"], line_data["message"])
            self.annotate_line(line_data["level"], line_data["type"], line_data["message"])
        else:
            print(line)

class BackupParser(DuplicacyLogParser):
    def __init__(self, plugin):
        DuplicacyLogParser.__init__(self, plugin)
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

    def log_line(self, level, type, message):
        if type == "UPLOAD_PROGRESS":
            return
        DuplicacyLogParser.log_line(self, level, type, message)

    def annotate_line(self, level, type, message):
        if type == "UPLOAD_PROGRESS":
            match = self.percent_re.search(message)
            if match:
                self.plugin.set_progress(float(match.group("percent")) / 100)
        elif type == "BACKUP_STATS":
            for key in self.stats_re:
                match = self.stats_re[key].search(message)
                if match:
                    self.stats[key] = { "count": match.group("count"), "size": match.group("size") }

    def process_complete(self, code):
        if code != 0:
            return DuplicacyLogParser.process_complete(self, code)

        if len(self.stats) > 0:
            rows = []
            for stat in ["new_files", "changed_files", "unchanged_files", "removed_files", "file_chunks", "metadata_chunks"]:
                if stat in self.stats:
                    rows.append([self.stats_names[stat], self.stats[stat]["count"], self.stats[stat]["size"]])

            self.plugin.log_table("Backup statistics", ["Type", "Count", "Size"], rows)

class CopyParser(DuplicacyLogParser):
    def __init__(self, plugin):
        DuplicacyLogParser.__init__(self, plugin)
        self.progress_re = re.compile(r"\((?P<done>\d+)/(?P<total>\d+)\)")

    def annotate_line(self, level, type, message):
        if type == "SNAPSHOT_COPY":
            match = self.progress_re.search(message)
            if match:
                self.plugin.set_progress(float(match.group("done")) / float(match.group("total")))

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

class DuplicacyPlugin(CroniclePlugin):
    def build_args(self, duplicacy, command, arguments):
        args = [duplicacy, "-debug", "-log", command]
        if command == "backup" or command == "check":
            args.append("-stats")
        if len(arguments) > 0:
            args.extend(arguments.split())
        return args

    def execute(self, params):
        args = self.build_args(params["duplicacy"], params["command"], params["arguments"])

        if params["command"] not in command_parsers:
            parser = DuplicacyLogParser(self)
        else:
            parser = command_parsers[params["command"]](self)

        self.exec_process(args, parser, cwd=params["repository"])

if __name__ == "__main__":
    DuplicacyPlugin()
