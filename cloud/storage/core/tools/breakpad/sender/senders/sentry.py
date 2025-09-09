from urllib.parse import urlparse
from uuid import uuid4
from datetime import datetime, timezone

import json
import requests

from .base import BaseSender, CrashInfoProcessed


class SentryFormatter(object):
    def __init__(self):
        return

    def _get_envelope_header(self, event_id):
        envelope_header = {
            "event_id": event_id,
            "sent_at": str(datetime.now(timezone.utc))
        }
        return envelope_header

    def _get_event_payload(self, event_id, crash: CrashInfoProcessed):
        event_payload = {
            "event_id": event_id,
            "logentry": {
                "message": f"{crash.service}: crash detected",
            },

            "timestamp": int(crash.time),
            "level": "fatal",
            "server_name": crash.server,
            "release": crash.metadata.get("release", "unknown"),
            "tags": {
                "service": crash.service,
                "image": crash.metadata.get("image", "unknown"),
                "cluster": crash.metadata.get("cluster", "unknown"),
            },
            "extra": {
                "backtrace": crash.formatted_backtrace,
            }
        }

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

    def _get_file_attachment(self, filepath: str):
        attachment_header, attachment_data = self._get_attachment(filepath)
        attachment_header_str = json.dumps(attachment_header) + "\n"

        return attachment_header_str.encode("utf-8") + attachment_data

    def create_event_envelope(self, crash: CrashInfoProcessed):
        event_id = uuid4().hex  # generate a unique event ID

        envelope_header = self._get_envelope_header(event_id)
        event_item_header = {"type": "event"}
        event_payload = self._get_event_payload(event_id, crash)

        # Construct the Envelope (consists of multiple newline-separated parts):
        # 1. Envelope Header (JSON)
        envelope_str = json.dumps(envelope_header) + "\n"
        # 2. Event Item Header (JSON)
        envelope_str += json.dumps(event_item_header) + "\n"
        # 3. Event Payload (JSON)
        envelope_str += json.dumps(event_payload) + "\n"

        envelope_bytes = envelope_str.encode("utf-8")

        if not crash.corefile or not crash.is_minidump:
            return envelope_bytes

        envelope_bytes += self._get_file_attachment(crash.corefile)
        envelope_bytes += "\n".encode("utf-8")
        envelope_bytes += self._get_file_attachment(crash.corefile + ".core")

        return envelope_bytes


class SentrySender(BaseSender):
    def __init__(self, dsn: str, ca_file: str, timeout: int):
        super().__init__()
        # Sentry DSN "https://<PUBLIC_KEY>@<SENTRY_HOST>/<PROJECT_ID>"
        dsn_parsed = urlparse(dsn)

        public_key = dsn_parsed.username
        sentry_host = dsn_parsed.hostname
        project_id = dsn_parsed.path.strip('/')

        self._endpoint_url = f"https://{sentry_host}/api/{project_id}/envelope/"
        self._sentry_auth = f"Sentry sentry_key={public_key}, sentry_client=custom-python-script/1.0"
        self._timeout = timeout
        self._ca_file = ca_file

    def _send_to_sentry(self, envelope_bytes: str, timeout: int):
        headers = {
            "Content-Type": "application/x-sentry-envelope",
            "X-Sentry-Auth": self._sentry_auth,
        }

        return requests.post(self._endpoint_url, data=envelope_bytes,
                             headers=headers, timeout=timeout, verify=self._ca_file)

    def send(self, crash: CrashInfoProcessed):
        formatter = SentryFormatter()
        event_envelope = formatter.create_event_envelope(crash)

        return self._send_to_sentry(event_envelope, self._timeout)
