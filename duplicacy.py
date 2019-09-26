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
            self.plugin.log(line)

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

class CheckParser(DuplicacyLogParser):
    def __init__(self, plugin):
        DuplicacyLogParser.__init__(self, plugin)
        self.total_revisions = 1
        self.current_revision = 0
        self.initial_re = re.compile(r"""\d+ snapshots and (\d+) revisions""")
        self.revision_re = re.compile(r""".*chunks referenced by snapshot.*\((\d+)/(\d+)\)$""")
        self.file_re = re.compile(r"""\((\d+)/(\d+)\)$""")

    def log_revision_progress(self, current, total):
        self.total_revisions = total
        self.current_revision = current
        self.log_file_progress(0, 1)

    def log_file_progress(self, current, total):
        progress = (self.current_revision + float(current) / total) / self.total_revisions
        self.plugin.set_progress(progress)

    def annotate_line(self, level, type, message):
        if type == "SNAPSHOT_CHECK":
            match = self.revision_re.match(message)
            if match is not None:
                self.log_revision_progress(int(match.group(1)), int(match.group(2)))
            else:
                match = self.initial_re.match(message)
                if match is not None:
                    self.total_revisions = int(match.group(1))
                    print("TOTAL REVISIONS %d", self.total_revisions)
        elif type == "SNAPSHOT_VERIFY":
            match = self.file_re.match(message)
            if match is not None:
                self.log_file_progress(int(match.group(1)), int(match.group(2)))

command_parsers = {
    "backup": BackupParser,
    "copy": CopyParser,
    "check": CheckParser,
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
