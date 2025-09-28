from datetime import timedelta
from enum import Enum
from logging import getLogger
from requests import HTTPError

from library.python.retry import retry

from .base import BaseSender, CrashInfoProcessed
from .cores import CoresSender
from .sentry import SentrySender
from .email import EmailSender

logger = getLogger(__name__)


class SenderError(Exception):
    pass


class AggregatorType(str, Enum):
    Cores = 'cores'
    Sentry = 'sentry'

    def __str__(self) -> str:
        return self.value


class DurableMultiSender(BaseSender):
    def __init__(self, aggregator_type, aggregator_url, project, emails,
                 ca_file):
        super().__init__()
        self._logger = logger.getChild(self.__class__.__name__)
        self._aggregator_url = aggregator_url
        self._aggregator_type = aggregator_type

        AGGREGATOR_TIMEOUT = 60  # Seconds
        if aggregator_type == AggregatorType.Cores:
            self._aggregator_sender = \
                CoresSender(aggregator_url, AGGREGATOR_TIMEOUT)
        elif aggregator_type == AggregatorType.Sentry:
            self._aggregator_sender = \
                SentrySender(aggregator_url, ca_file, AGGREGATOR_TIMEOUT)
        else:
            raise SenderError(f"Unknown aggregator: {aggregator_type}")

        self._email_sender = None
        if emails:
            self._email_sender = EmailSender(self._logger, emails)

    @retry(max_times=10, max_time=timedelta(seconds=30))
    def _do_send_to_aggregator(self, crash: CrashInfoProcessed):
        self._logger.info(f"Sending crash info to aggregator "
                          f"{self._aggregator_type}: {self._aggregator_url}")

        response = None
        try:
            response = self._aggregator_sender.send(crash)
            response.raise_for_status()
        except HTTPError as e:
            raise SenderError("Error sending crash to aggregator") from e

        self._logger.info("Get response from aggregator: %r", response.text)

    def _send_to_aggregator(self, crash: CrashInfoProcessed):
        try:
            self._do_send_to_aggregator(crash)
        except Exception as e:
            raise SenderError(
                "Gave up on sending core dump to the aggregator") from e

    @retry(max_times=10, max_time=timedelta(seconds=30))
    def _send_email(self, crash: CrashInfoProcessed):
        if self._email_sender:
            self._email_sender.send(crash)

    def _write_to_logfile(self, crash: CrashInfoProcessed):
        try:
            self._logger.info("Write crash info to logfile %r", crash.logfile)
            with open(crash.logfile, "a+") as fd:
                fd.write("\n" + crash.get_header() + "\n")
                if crash.core_url:
                    fd.write("URL: " + self._aggregator_url + "\n")
                fd.write(crash.formatted_backtrace + "\n")
        except IOError as e:
            self._logger.error(
                "Can't write info to file %s %r", crash.logfile, e)
            self._logger.debug("Exception", exc_info=True)

    def _write_to_stdout(self, crash: CrashInfoProcessed):
        self._logger.info("%s", crash.get_header())
        if crash.core_url:
            self._logger.info("URL %s", crash.core_url)

    def send(self, crash: CrashInfoProcessed):
        if self._aggregator_sender:
            self._send_to_aggregator(crash)

        if self._email_sender:
            self._send_email(crash)

        if crash.logfile:
            self._write_to_logfile(crash)

        self._write_to_stdout(crash)
