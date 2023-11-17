from cloud.blockstore.tools.cms.lib.conductor import ConductorMock
from cloud.blockstore.tools.cms.lib.config_main import do_main
from cloud.blockstore.tools.cms.lib.pssh import PsshMock

if __name__ == '__main__':
    do_main(ConductorMock(), PsshMock(), '/Berkanavt/nbs-server/cfg')
