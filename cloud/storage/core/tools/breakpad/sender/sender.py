# -*- coding: UTF-8 -*-

import logging
import socket
import subprocess
from email.mime.text import MIMEText

import requests
from library.python.retry import retry

from .conductor import Conductor
from .coredump_formatter import CoredumpFormatter
from .sentry import SentrySender, SentryFormatter

logger = logging.getLogger(__name__)


class SenderError(Exception):
    pass


class Sender(object):
    CRASH_TYPE_CORE = "crash"
    CRASH_TYPE_OOM = "oom"
    AGGREGATOR_TYPE_CORES = 'cores'
    AGGREGATOR_TYPE_SENTRY = 'sentry'
    AGGREGATOR_TIMEOUT = 60  # Seconds

    def __init__(self, aggregator_type, aggregator_url, project, emails,
                 ca_file):
        super(Sender, self).__init__()
        self._logger = logger.getChild(self.__class__.__name__)
        self.aggregator_type = aggregator_type
        self.aggregator_url = aggregator_url
        self.project = project
        self.emails = emails
        self.ca_file = ca_file

        self._formatted_coredump = None
        self._formatted_current_thread = None
        self._core_url = None
        self._metadata = None
        self.core_file = None
        self.coredump = None
        self.info = None
        self.service_name = None
        self.crash_type = None
        self.timestamp = None
        self.logfile = None
        self.server = socket.getfqdn() or "unknown"

    def _collect_metadata(self):
        cgroup = Conductor().primary_group \
            if self.aggregator_type == self.AGGREGATOR_TYPE_CORES else None
        self._metadata = dict(
            ctype=cgroup if cgroup else "unknown",
            server=self.server,
            service=self.service_name,
            time=str(self.timestamp),
        )

    def _format_coredump(self):
        formatter = CoredumpFormatter(self.coredump)
        self._formatted_coredump = formatter.format()
        self._formatted_current_thread = formatter.format_current_thread()

    def _header(self):
        return "Process {service} {crash_type}ed" \
               "on {server} cluster {ctype}".format(
                   crash_type=self.crash_type,
                   **self._metadata
               )

    def _get_coredump_with_info(self):
        if not self.info:
            return self.coredump
        return self.info + "\n" + self.coredump

    def _post_crash_to_cores(self):
        url = self.aggregator_url + "/corecomes"
        return requests.post(
            url, params=self._metadata, data=self._get_coredump_with_info(),
            timeout=self.AGGREGATOR_TIMEOUT)

    def _post_crash_to_sentry(self):
        timestamp = int(self.timestamp)
        formatter = SentryFormatter()
        event_envelope = formatter.create_event_envelope(
            self.service_name, timestamp, self.server,
            self.core_file, self._formatted_current_thread)
        sender = SentrySender(self.aggregator_url, self.ca_file)
        return sender.send(event_envelope, timeout=self.AGGREGATOR_TIMEOUT)

    @retry(max_times=10, delay=60)
    def _do_send_to_aggregator(self):
        url = self.aggregator_url
        self._logger.info("Sending crash info to aggregator %s", url)

        if self.aggregator_type == self.AGGREGATOR_TYPE_CORES:
            response = self._post_crash_to_cores()
        elif self.aggregator_type == self.AGGREGATOR_TYPE_SENTRY:
            response = self._post_crash_to_sentry()
        else:
            raise SenderError(f"Unknown aggregator: {self.aggregator_type}")

        if response.status_code != 200:
            msg = f"Error sending crash to aggregator {url}: \
                code {response.status_code}, {response.text}"
            self._logger.error(msg)
            raise SenderError(msg)

        self._logger.info("Get response from aggregator: %r", response.text)

    def _send_to_aggregator(self):
        try:
            self._do_send_to_aggregator()
        except Exception as e:
            self._logger.error(
                "gave up on sending core dump to the aggregator: %r", e)
            pass

    @retry(max_times=10, delay=60)
    def _send_email(self):
        self._logger.info("Send core to email %r", self.emails)
        mail_from = "devnull@example.com"
        mail_to = ", ".join(self.emails)
        message_body = [self._header()]
        if self._core_url:
            message_body.append("URL: " + self._core_url)
        if self.info:
            message_body.append("")
            message_body.append(self.info)
        if self.crash_type != self.CRASH_TYPE_OOM:
            message_body.append("")
            message_body.append(self._formatted_coredump)

        message = MIMEText("\n".join(message_body))
        message["Subject"] = \
            "[{ctype}] {crash_type} {service} on {server}".format(
                crash_type=self.crash_type,
                **self._metadata
            )
        message["From"] = "{server} <{address}>".format(
            server=self._metadata.get("server", "unknown"),
            address=mail_from
        )
        message["To"] = mail_to

        try:
            sendmail = subprocess.Popen(
                ["/usr/sbin/sendmail", "-t"], stdin=subprocess.PIPE)
            sendmail.communicate(message.as_string().encode("utf-8"))
            sendmail.wait()
        except Exception as e:
            self._logger.error("sendmail error %r", e)
            self._logger.debug("Exception", exc_info=True)

    def _write_to_logfile(self):
        try:
            self._logger.info("Write crash info to logfile %r", self.logfile)
            with open(self.logfile, "a+") as fd:
                fd.write("\n" + self._header() + "\n")
                if self._core_url:
                    fd.write("URL: " + self._core_url + "\n")
                fd.write(self._formatted_coredump + "\n")
        except IOError as e:
            self._logger.error(
                "Can't write info to file %s %r", self.logfile, e)
            self._logger.debug("Exception", exc_info=True)

    def _write_to_stdout(self):
        self._logger.info("%s", self._header())
        if self._core_url:
            self._logger.info("URL %s", self._core_url)

    def send(self, timestamp, core_file, coredump, info, service_name,
             crash_type, logfile):
        self._core_url = None
        self.timestamp = timestamp
        self.core_file = core_file
        self.coredump = coredump
        self.info = info
        self.service_name = service_name or "unknown"
        self.crash_type = crash_type
        self.logfile = logfile

        self._collect_metadata()
        self._format_coredump()

        if self.aggregator_url:
            self._send_to_aggregator()

        if self.emails:
            self._send_email()

        if self.logfile:
            self._write_to_logfile()

        self._write_to_stdout()
