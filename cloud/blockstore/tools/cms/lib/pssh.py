
class PsshMock(object):

    def __init__(self, runner=None):
        self._runner = runner

    def scp(self, host, src, dst, attempts=10):
        return None

    def scp_to(self, host, src, dst, attempts=10):
        return None

    def run(self, cmd, target, attempts=10):
        return self._runner(cmd, target, attempts)

    def resolve(self, query):
        return None
