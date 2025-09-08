from requests import post

from .base import BaseSender, CrashInfoProcessed


class CoresSender(BaseSender):
    def __init__(self, aggregator_url: str, timeout: int):
        super().__init__()
        self._aggregator_url = aggregator_url
        self._timeout = timeout

    def _get_metadata(self, crash: CrashInfoProcessed):
        return dict(
            ctype=crash.cluster,
            server=crash.server,
            service=crash.service,
            time=str(crash.time),
        )

    def _get_backtrace_with_info(self, crash: CrashInfoProcessed):
        if not crash.info:
            return crash.backtrace
        return crash.info + "\n" + crash.backtrace

    def send(self, crash: CrashInfoProcessed):
        url = self._aggregator_url + "/corecomes"
        return post(
            url,
            params=self._get_metadata(),
            data=self._get_backtrace_with_info(crash),
            timeout=self._timeout)

