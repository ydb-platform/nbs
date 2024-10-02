import requests
import time

from cloud.filestore.config.server_pb2 import TServerAppConfig
from cloud.filestore.config.storage_pb2 import TStorageConfig
from cloud.filestore.tests.python.lib.server import NfsServer, wait_for_nfs_server
from cloud.filestore.tests.python.lib.server import wait_for_filestore_vhost
from cloud.filestore.tests.python.lib.daemon_config import NfsServerConfigGenerator
from cloud.filestore.tests.python.lib.daemon_config import NfsVhostConfigGenerator

from contrib.ydb.core.protos import config_pb2
from contrib.ydb.core.protos.config_pb2 import TLogConfig

from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

import yatest.common as yatest_common


def setup_kikimr():
    kikimr_binary_path = yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd")

    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_path=kikimr_binary_path,
        use_in_memory_pdisks=True,
        dynamic_storage_pools=[
            dict(name="dynamic_storage_pool:1", kind="hdd", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0)
        ])

    return configurator


def setup_filestore(configurator, kikimr_cluster, binary_path, configurator_type):
    kikimr_port = list(kikimr_cluster.nodes.values())[0].port

    storage_config = TStorageConfig()
    storage_config.ConfigsDispatcherServiceEnabled = True
    storage_config.ConfigDispatcherSettings.AllowList.Names.append('NameserviceConfigItem')

    domain = configurator.domains_txt.Domain[0].Name

    return configurator_type(
        binary_path=binary_path,
        app_config=TServerAppConfig(),
        service_type="kikimr",
        verbose=True,
        kikimr_port=kikimr_port,
        domain=domain,
        storage_config=storage_config,
    )


def check_log_config(nfs_configurator):
    def query_monitoring(url):
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.text

    logger_page = query_monitoring(f'http://localhost:{nfs_configurator.mon_port}/actors/logger')

    log_config = nfs_configurator.log_config

    level_to_name = {4: 'WARN', 5: 'NOTICE', 6: 'INFO', 7: 'DEBUG', 8: 'TRACE'}

    for e in log_config.Entry:
        assert e.Level in level_to_name
        pattern = f'{e.Component.decode()}</a></td><td>{level_to_name[e.Level]}</td>'
        assert logger_page.find(pattern) != -1


def check_configs(nfs_configurator, kikimr_configurator, kikimr_cluster):
    check_log_config(nfs_configurator)

    app_config = config_pb2.TAppConfig()

    # add new log entry
    component_to_test = 'NFS_SERVER'.encode()
    app_config = config_pb2.TAppConfig()
    log_config = TLogConfig()
    entry = log_config.Entry.add()
    entry.Component = component_to_test
    entry.SamplingRate = 1000
    app_config.LogConfig.MergeFrom(log_config)
    kikimr_cluster.client.add_config_item(app_config)

    # add new static node
    app_config = config_pb2.TAppConfig()
    naming_config = kikimr_configurator.names_txt
    node = naming_config.Node.add(
        NodeId=2,
        Address='::1',
        Port=65535,
        Host='somewhere',
    )
    node.WalleLocation.DataCenter = 'xyz'
    node.WalleLocation.Rack = 'somewhere'
    node.WalleLocation.Body = 1
    app_config.NameserviceConfig.MergeFrom(naming_config)
    kikimr_cluster.client.add_config_item(app_config)

    def query_monitoring(url, text):
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.text.find(text) != -1

    # wait for nameservice config update
    while True:
        if query_monitoring(f'http://localhost:{nfs_configurator.mon_port}/actors/dnameserver', 'somewhere'):
            break
        else:
            time.sleep(10)

    # check that logging config was not changed
    check_log_config(nfs_configurator)


def setup_and_run_test(filestore_binary_path, filestore_config_generator, wait_filestore_process):
    kikimr_configurator = setup_kikimr()

    kikimr_cluster = kikimr_cluster_factory(configurator=kikimr_configurator)
    kikimr_cluster.start()

    filestore_configurator = setup_filestore(
        kikimr_configurator,
        kikimr_cluster,
        filestore_binary_path,
        filestore_config_generator)
    filestore_configurator.generate_configs(
        kikimr_configurator.domains_txt,
        kikimr_configurator.names_txt)

    filestore_process = NfsServer(configurator=filestore_configurator)
    filestore_process.start()

    try:
        wait_filestore_process(filestore_process, filestore_configurator.port)
    except RuntimeError:
        assert False, 'unable to start filestore process'

    check_configs(
        filestore_configurator,
        kikimr_configurator,
        kikimr_cluster)

    filestore_process.stop()


def test_server():
    filestore_binary_path = yatest_common.binary_path('cloud/filestore/apps/server/filestore-server')
    setup_and_run_test(filestore_binary_path, NfsServerConfigGenerator, wait_for_nfs_server)


def test_vhost():
    filestore_binary_path = yatest_common.binary_path('cloud/filestore/apps/vhost/filestore-vhost')
    setup_and_run_test(filestore_binary_path, NfsVhostConfigGenerator, wait_for_filestore_vhost)
