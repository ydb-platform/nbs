import requests
import time

from cloud.blockstore.tests.python.lib.config import NbsConfigurator, generate_disk_agent_txt
from cloud.blockstore.tests.python.lib.daemon import start_ydb, start_nbs, start_disk_agent

from contrib.ydb.core.protos import config_pb2
from contrib.ydb.core.protos.config_pb2 import TLogConfig


def verify_config_update(ydb, mon_port):

    def query_monitoring(url, text):
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.text.find(text) != -1

    assert query_monitoring(
        f'http://localhost:{mon_port}/actors/logger?c=1025',
        'Sampling rate: 0')

    app_config = config_pb2.TAppConfig()

    # add new log entry
    component_to_test = 'BLOCKSTORE_SERVER'.encode()
    app_config = config_pb2.TAppConfig()
    log_config = TLogConfig()
    entry = log_config.Entry.add()
    entry.Component = component_to_test
    entry.SamplingRate = 1000
    app_config.LogConfig.MergeFrom(log_config)
    ydb.client.add_config_item(app_config)

    # add new static node
    app_config = config_pb2.TAppConfig()
    naming_config = ydb.config.names_txt
    node = naming_config.Node.add(
        NodeId=2,
        Address='::1',
        Port=65535,
        Host='somewhere',
    )
    node.Location.DataCenter = 'xyz'
    node.Location.Rack = 'somewhere'
    app_config.NameserviceConfig.MergeFrom(naming_config)
    ydb.client.add_config_item(app_config)

    def query_monitoring(url, text):
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.text.find(text) != -1

    # wait for nameservice config update
    while True:
        if query_monitoring(f'http://localhost:{mon_port}/actors/dnameserver', 'somewhere'):
            break
        else:
            time.sleep(10)

    # check that logging config was not changed
    result = query_monitoring(
        f'http://localhost:{mon_port}/actors/logger?c=1025',
        'Sampling rate: 0')

    return result


def prepare(ydb, node_type, disable_local_service):
    nbs_configurator = NbsConfigurator(ydb)
    nbs_configurator.generate_default_nbs_configs()

    nbs_configurator.files['storage'].NodeType = node_type
    nbs_configurator.files['storage'].DisableLocalService = disable_local_service
    nbs_configurator.files['storage'].ConfigsDispatcherServiceEnabled = True
    nbs_configurator.files['storage'].ConfigDispatcherSettings.AllowList.Names.append('NameserviceConfigItem')

    return nbs_configurator


def setup_and_run_test_for_server(node_type, disable_local_service):
    ydb = start_ydb()

    nbs = start_nbs(prepare(ydb, node_type, disable_local_service))
    result = verify_config_update(ydb, nbs.mon_port)
    nbs.kill()

    return result


def setup_and_run_test_for_da():
    ydb = start_ydb()

    nbs = start_nbs(prepare(ydb, 'nbs_control', False))

    da_configurator = prepare(ydb, 'disk-agent', True)
    da_configurator.files["disk-agent"] = generate_disk_agent_txt(agent_id='')
    da = start_disk_agent(da_configurator)

    result = verify_config_update(ydb, da.mon_port)

    da.kill()
    nbs.kill()

    return result


def test_nbs_control_config_update():
    assert setup_and_run_test_for_server('nbs_control', False) is True


def test_server_config_update():
    assert setup_and_run_test_for_server('nbs', True) is True


def test_da_config_update():
    assert setup_and_run_test_for_da() is True
