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
        envelope_header_dict = {
            "event_id": event_id,
            "sent_at": str(datetime.now(timezone.utc))
        }
        envelope_header = json.dumps(envelope_header_dict).encode("utf-8")
        return envelope_header

    def _get_event(self, event_id: str, crash: CrashInfoProcessed):
        event_payload_dict = {
            "event_id": event_id,
            "platform": "native",
            "message": f"{crash.service}: crash detected",
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
        event_payload = json.dumps(event_payload_dict).encode("utf-8")

        event_header_dict = {
            "type": "event",
            "content_type": "application/json",
            "length": len(event_payload)
        }
        event_header = json.dumps(event_header_dict).encode("utf-8")

        return event_header, event_payload

    def _get_attachment(self, filepath: str, type: str):
        try:
            with open(filepath, "rb") as f:
                attachment_data = f.read()
        except Exception as e:
            attachment_data = \
                f"Error occured while reading file" \
                f"{filepath}: {str(e)}".encode(encoding="utf-8")

        attachment_header_dict = {
            "type": "attachment",
            "attachment_type": f"{type}",
            "length": len(attachment_data),
            "filename": filepath,
            "content_type": "text/plain",
        }
        attachment_header = json.dumps(attachment_header_dict).encode("utf-8")

        return attachment_header, attachment_data

    def create_event_envelope(self, crash: CrashInfoProcessed):
        event_id = uuid4().hex  # generate a unique event ID

        envelope_header = self._get_envelope_header(event_id)
        event_header, event_payload = self._get_event(event_id, crash)

        # Construct the Envelope (consists of multiple newline-separated parts):
        parts = [
            envelope_header,
            event_header,
            event_payload,
        ]

        if crash.corefile and crash.is_minidump:
            minidump_path = crash.corefile
            core_path = crash.corefile + ".core"

            minidump_header, minidump_payload = \
                self._get_attachment(minidump_path, "event.minidump")

            core_header, core_payload = \
                self._get_attachment(core_path, "event.attachment")

            parts += [
                minidump_header,
                minidump_payload,
                core_header,
                core_payload,
            ]

        return b"\n".join(parts)


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

        response = requests.post(
            self._endpoint_url, data=envelope_bytes, headers=headers,
            timeout=timeout, verify=self._ca_file)
        response.raise_for_status()

        return response

    def send(self, crash: CrashInfoProcessed):
        formatter = SentryFormatter()
        event_envelope = formatter.create_event_envelope(crash)

        return self._send_to_sentry(event_envelope, self._timeout)
