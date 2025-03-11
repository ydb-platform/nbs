from urllib.parse import urlparse
from uuid import uuid4
from datetime import datetime, timezone

import json
import re
import requests


class SentryFormatter(object):
    def __init__(self):
        return

    def convert_gdb_backtrace_to_sentry_frames(self, backtrace):
        # Regular expression pattern to capture stack frame details.
        # This regex matches:
        # - A leading frame number: "#<number>"
        # - Optionally an address and the keyword "in"
        # - A function name (captured up to the first '(')
        # - Optionally function arguments in parentheses (ignored here)
        # - Optionally, a file and line number prefixed by "at"
        FRAME_RE = re.compile(
            r"^#(?P<frame_no>\d+)\s+"
            r"(?:(?P<address>0x[0-9a-fA-F]+)\s+in\s+)?"
            r"(?P<function>[^(]+)"
            r"(?:\([^)]*\))?\s*"
            r"(?:at\s+(?P<filename>[^:]+):(?P<lineno>\d+))?",
            re.MULTILINE
        )

        frames = []
        for line in backtrace.splitlines():
            line = line.strip()

            if not line or line.startswith('['):
                continue

            match = FRAME_RE.match(line)
            if match:
                frame = match.groupdict()

                if frame.get("lineno"):
                    frame["lineno"] = int(frame["lineno"])
                else:
                    frame["lineno"] = None

                if frame.get("function"):
                    frame["function"] = frame["function"].strip()

                frame["in_app"] = True
                frames.append(frame)

        frames.reverse()
        return frames

    def _get_envelope_header(self, event_id):
        envelope_header = {
            "event_id": event_id,
            "sent_at": str(datetime.now(timezone.utc))
        }
        return envelope_header

    def _get_event_payload(self, event_id, service, timestamp, server, backtrace):
        event_payload = {
            "event_id": event_id,
            "logentry": {
                "message": f"{service}: crash detected",
            },

            "timestamp": int(timestamp),
            "level": "fatal",
            "server_name": server,
            "release": "unknown",
            "tags": {
                "service": service,
            },
            "extra": {
                "backtrace": backtrace,
            }
        }

        sentry_frames = self.convert_gdb_backtrace_to_sentry_frames(backtrace)
        if sentry_frames:
            event_payload["stacktrace"] = {"frames": sentry_frames}
        return event_payload

    def _get_attachment(self, filepath: str):
        try:
            with open(filepath, "rb") as f:
                attachment_data = f.read()
        except Exception as e:
            attachment_data = \
                f"Error occured while reading file" \
                f"{filepath}: {str(e)}".encode(encoding="utf-8")

        attachment_length = len(attachment_data)
        attachment_item_header = {
            "type": "attachment",
            "length": attachment_length,
            "filename": filepath,
            "content_type": "text/plain"
        }
        return attachment_item_header, attachment_data

    def create_event_envelope(self, service, timestamp, server,
                              minidump_path: str, backtrace: str):
        event_id = uuid4().hex  # generate a unique event ID

        envelope_header = self._get_envelope_header(event_id)
        event_item_header = {"type": "event"}
        event_payload = self._get_event_payload(
            event_id, service, timestamp, server, backtrace)

        # Construct the Envelope (consists of multiple newline-separated parts):
        # 1. Envelope Header (JSON)
        envelope_str = json.dumps(envelope_header) + "\n"
        # 2. Event Item Header (JSON)
        envelope_str += json.dumps(event_item_header) + "\n"
        # 3. Event Payload (JSON)
        envelope_str += json.dumps(event_payload) + "\n"

        if not minidump_path:
            return envelope_str.encode("utf-8")

        # 4. Attachment Item Header (JSON)
        attachment_header, attachment_data = self._get_attachment(minidump_path)
        envelope_str += json.dumps(attachment_header) + "\n"
        # 5. Attachment Payload (raw bytes)
        envelope_bytes = envelope_str.encode("utf-8") + attachment_data

        return envelope_bytes


class SentrySender(object):
    def __init__(self, dsn: str, ca_file: str):
        # Sentry DSN "https://<PUBLIC_KEY>@<SENTRY_HOST>/<PROJECT_ID>"
        dsn_parsed = urlparse(dsn)

        public_key = dsn_parsed.username
        sentry_host = dsn_parsed.hostname
        project_id = dsn_parsed.path.strip('/')

        self.endpoint_url = f"https://{sentry_host}/api/{project_id}/envelope/"
        self.sentry_auth = f"Sentry sentry_key={public_key}, sentry_client=custom-python-script/1.0"
        self.ca_file = ca_file

    def send(self, envelope_bytes: str, timeout: float):
        headers = {
            "Content-Type": "application/x-sentry-envelope",
            "X-Sentry-Auth": self.sentry_auth,
        }

        return requests.post(self.endpoint_url, data=envelope_bytes,
                             headers=headers, timeout=timeout, verify=self.ca_file)
