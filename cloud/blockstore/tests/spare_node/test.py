import pytest

from cloud.blockstore.public.sdk.python.client import CreateClient

from cloud.blockstore.tests.python.lib.config import NbsConfigurator
from cloud.blockstore.tests.python.lib.daemon import start_ydb, start_nbs, get_fqdn


@pytest.fixture(name='ydb')
def start_ydb_cluster():

    ydb_cluster = start_ydb()

    yield ydb_cluster

    ydb_cluster.stop()


def test_spare_node(ydb):
    nbs_configurator = NbsConfigurator(ydb)
    nbs_configurator.generate_default_nbs_configs()

    nbs_configurator.files["storage"].DisableLocalService = True
    nbs_configurator.files["storage"].KnownSpareNodes.append(get_fqdn())

    nbs = start_nbs(nbs_configurator)

    client = CreateClient(f"localhost:{nbs.port}")

    r = client.query_available_storage(["unknown"])

    assert len(r) == 0

    is_spare_node = nbs.counters.find({'sensor': 'IsSpareNode'})

    assert is_spare_node['value'] == 1

    nbs.kill()
