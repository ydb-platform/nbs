from abc import abstractmethod, ABC

from ...common.crash_info import CrashInfoProcessed


class BaseSender(ABC):
    @abstractmethod
    def send(self, crash: CrashInfoProcessed):
        pass
