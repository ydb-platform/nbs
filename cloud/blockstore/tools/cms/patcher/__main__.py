from cloud.blockstore.tools.cms.lib.conductor import ConductorMock
from cloud.blockstore.tools.cms.lib.patcher_main import do_main, ModuleFactories
from cloud.blockstore.tools.cms.lib.pssh import PsshMock

import sys


def make_conductor(test_data_dir: str):
    return ConductorMock(test_data_dir)


def make_pssh(robot: bool, cluster: str):
    return PsshMock()


if __name__ == '__main__':
    sys.exit(do_main(
        ModuleFactories(make_conductor, make_pssh),
        '/Berkanavt/nbs-server/cfg'))
