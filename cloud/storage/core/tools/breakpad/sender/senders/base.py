from ...common.crash_info import CrashInfoProcessed


class BaseSender(object):
    def send(self, crash: CrashInfoProcessed):
        pass
