# -*- coding: UTF-8 -*-

import argparse
import json
import logging
import os
import signal
import sys
import time

from ..common.crash_info import CrashInfoStorage
from .crash_processor import (
    CrashProcessorError, CoredumpCrashProcessor, OOMCrashProcessor)
from .limiter import Limiter
from .senders.durable_multi import (
    AggregatorType, DurableMultiSender, SenderError)

logger = logging.getLogger(__name__)


class BreakpadSender:
    EMPTY_QUEUE_WAIT_TIME = 5.0

    def __init__(self):
        self._logger = logger.getChild(self.__class__.__name__)
        self._storage = None
        self._stopping = False
        self._sender = None
        self._limiter = None
        self._args = None
        self._config = {}

    def _parse_args(self):
        parser = argparse.ArgumentParser(
            description="Crash dump processor",
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        parser.add_argument("--datadir", type=str, default="/var/tmp/breakpad",
                            metavar="DIR",
                            help="breakpad-launcher data directory")
        parser.add_argument("--aggregator-type", type=AggregatorType,
                            metavar="TYPE",
                            choices=list(AggregatorType),
                            default=AggregatorType.Cores,
                            help="Aggregator type, one of: [%(choices)s]")
        parser.add_argument("--aggregator-url", type=str,
                            default="http://cores.cloud-preprod.yandex.net",
                            metavar="URL")
        parser.add_argument("--prj", type=str, default="nbs",
                            help="Project tag")
        parser.add_argument("--limit-window", type=int, default=10*60,
                            help="Limit window, seconds")
        parser.add_argument("--limit-cores", type=int, default=5,
                            help="Limit cores per window")
        parser.add_argument("--config", type=str, metavar="PATH")
        parser.add_argument("--verbose", action="store_true")
        parser.add_argument("--ca-file", type=str,
                            help="Optional certificate authority file (*.pem)")
        parser.add_argument("--gdb-timeout", type=int, default=300,
                            help="GDB timeout in seconds")
        parser.add_argument("--gdb-disabled", action="store_true",
                            help="Disable running gdb (just send minidump)")

        # TODO: remove, kept for backward compatibility
        parser.add_argument("--nbs-config", type=str, metavar="PATH")
        self._args = parser.parse_args()

    def set_signals(self):
        signal.signal(signal.SIGINT, self.on_signal)
        signal.signal(signal.SIGTERM, self.on_signal)
        signal.signal(signal.SIGHUP, self.on_signal)

    def on_signal(self, signum, _):
        self._logger.info("signal %s received", signum)
        self._stopping = True

    def run_crash_queue_loop(self):
        while not self._stopping:
            crash_info = self._storage.get()
            if not crash_info:
                time.sleep(self.EMPTY_QUEUE_WAIT_TIME)
                continue

            if not self._limiter.check():
                self._logger.info("cores limit exceeded, skip core %r",
                                  crash_info.time)
                continue

            try:
                if crash_info.corefile:
                    processor = CoredumpCrashProcessor(self._args.gdb_timeout,
                                                       self._args.gdb_disabled)
                else:
                    processor = OOMCrashProcessor()

                crash = processor.process(crash_info)
                self._sender.send(crash)
            except CrashProcessorError:
                self._logger.exception(f"Can't process core "
                                       f"{crash_info.corefile}")
                continue
            except SenderError:
                self._logger.exception(f"Can't send crash info {crash}")
                continue
            except Exception:
                self._logger.exception("Unexpected error happened")
                continue

            processor.cleanup()

    def load_config(self):
        if not self._args.nbs_config and not self._args.config:
            return

        try:
            config = self._args.nbs_config if self._args.nbs_config \
                else self._args.config
            self._logger.info("Load config from %s", config)
            with open(config, "r") as fd:
                self._config = json.load(fd)
        except (IOError, OSError) as e:
            self._logger.error("Error reading config %r", e)

    def get_config_emails(self):
        notify_emails = self._config.get("notify_email")
        emails = notify_emails.split(',') if notify_emails else list()

        unique_emails = list(set(emails))
        if unique_emails:
            self._logger.info("Notify emails: %r", unique_emails)

        return unique_emails

    def init(self):
        self.set_signals()
        self._storage = \
            CrashInfoStorage(os.path.join(self._args.datadir, "queue"))

        self.load_config()
        emails = self.get_config_emails()
        type = self._config.get("aggregator_type", self._args.aggregator_type)
        url = self._config.get("aggregator_url", self._args.aggregator_url)
        ca_file = self._config.get("ca_file", self._args.ca_file)

        self._sender = DurableMultiSender(type, url, self._args.prj, emails, ca_file)
        self._limiter = Limiter(
            window_seconds=self._args.limit_window,
            limit=self._args.limit_cores)

    def run(self):
        logging.basicConfig(format="%(asctime)s - %(levelname)s - %(message)s",
                            level=logging.INFO)
        self._parse_args()

        if self._args.verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        try:
            self.init()
            self.run_crash_queue_loop()
        except KeyboardInterrupt:
            return 0
        except Exception as e:
            self._logger.exception(e)
            return 1
        return 0


def main():
    sys.exit(BreakpadSender().run())
