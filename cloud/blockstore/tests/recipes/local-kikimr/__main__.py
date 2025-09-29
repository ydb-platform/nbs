import argparse
import json
import os
import signal
import logging
import uuid

from library.python.testing.recipe import declare_recipe, set_env

from cloud.blockstore.config.server_pb2 import TServerConfig, TServerAppConfig, TKikimrServiceConfig
from cloud.blockstore.config.discovery_pb2 import TDiscoveryServiceConfig
from cloud.blockstore.config.cells_pb2 import TCellsConfig, TCellConfig, TCellHostConfig, ECellDataTransport

from cloud.blockstore.tests.python.lib.nbs_runner import LocalNbs
from cloud.blockstore.tests.python.lib.test_base import thread_count, wait_for_nbs_server, recipe_set_env

from contrib.ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from contrib.ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from cloud.storage.core.tests.common import (
    append_recipe_err_files,
    process_recipe_err_files,
)

import yatest.common as yatest_common


PID_FILE_NAME = "local_kikimr_nbs_server_recipe_{}.pid"
ERR_LOG_FILE_NAMES_FILE = "local_kikimr_nbs_server_recipe.err_log_files"

logger = logging.getLogger(__name__)


def create_cells_config(args, path, port, secure_port) -> TCellsConfig:
    if not args.use_cells:
        return None

    cells_config = TCellsConfig()
    cells_config.CellsEnabled = True
    cells_config.CellId = uuid.uuid4().hex[:20]

    file_already_exists = True
    if os.path.exists(path):
        with open(path, "r") as f:
            cells_meta = json.load(f)
    else:
        cells_meta = {'cells': []}
        file_already_exists = False

    cells_meta['cells'].append({
        'id': cells_config.CellId,
        'nbs_port': port,
        'nbs_secure_port': secure_port
    })

    with open(path, "w") as f:
        json.dump(cells_meta, f)

    if file_already_exists:
        return cells_config

    for cell in cells_meta['cells']:
        cell_config = TCellConfig()
        cell_config.CellId = cell['id']
        cell_config.GrpcPort = cell['nbs_port']
        cell_config.SecureGrpcPort = cell['nbs_secure_port']
        cell_config.Transport = ECellDataTransport.CELL_DATA_TRANSPORT_GRPC

        host = TCellHostConfig()
        host.Fqdn = "localhost"
        cell_config.Hosts.append(host)

        cells_config.Cells.append(cell_config)

    return cells_config


def pars_args(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--kikimr-package-path", action='store', default=None)
    parser.add_argument("--nbs-package-path", action='store', default=None)
    parser.add_argument("--use-log-files", action='store_true', default=False)
    parser.add_argument("--use-ic-version-check", action='store_true', default=False)
    parser.add_argument("--use-cells", action='store_true', default=False)
    parser.add_argument("--nbs-index", action='store', default=0)

    return parser.parse_args(argv)


def start(argv):
    args = pars_args(argv)

    kikimr_binary_path = yatest_common.binary_path("contrib/ydb/apps/ydbd/ydbd")
    if args.kikimr_package_path is not None:
        kikimr_binary_path = yatest_common.build_path(
            "{}/ydbd".format(args.kikimr_package_path)
        )

    nbs_binary_path = yatest_common.binary_path("cloud/blockstore/apps/server/nbsd")
    if args.nbs_package_path is not None:
        nbs_binary_path = yatest_common.build_path(
            "{}/usr/bin/blockstore-server".format(args.nbs_package_path)
        )

    pm = yatest_common.network.PortManager()
    clusters = []
    nbs_servers = []

    nbs_index = int(args.nbs_index)
    logger.info("starting instans No {}".format(nbs_index))
    configurator = KikimrConfigGenerator(
        erasure=None,
        binary_path=kikimr_binary_path,
        use_in_memory_pdisks=True,
        dynamic_storage_pools=[
            dict(name="dynamic_storage_pool:1", kind="hdd", pdisk_user_kind=0),
            dict(name="dynamic_storage_pool:2", kind="ssd", pdisk_user_kind=0)
        ],
        use_log_files=args.use_log_files)

    kikimr_cluster = kikimr_cluster_factory(configurator=configurator, sub_folder_name="kikimr_configs_{}".format(nbs_index))
    kikimr_cluster.start()
    clusters.append((configurator, kikimr_cluster))

    server_app_config = TServerAppConfig()
    server_app_config.ServerConfig.CopyFrom(TServerConfig())
    server_app_config.ServerConfig.ThreadsCount = thread_count()
    server_app_config.ServerConfig.StrictContractValidation = False
    server_app_config.ServerConfig.NbdEnabled = True
    server_app_config.ServerConfig.VhostEnabled = True
    server_app_config.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    certs_dir = yatest_common.source_path('cloud/blockstore/tests/certs')
    set_env("TEST_CERT_FILES_DIR", certs_dir)

    server_app_config.ServerConfig.RootCertsFile = os.path.join(certs_dir, 'server.crt')
    cert = server_app_config.ServerConfig.Certs.add()
    cert.CertFile = os.path.join(certs_dir, 'server.crt')
    cert.CertPrivateKeyFile = os.path.join(certs_dir, 'server.key')

    nbs_port = pm.get_port()
    nbs_secure_port = pm.get_port()

    instance_list_file = os.path.join(yatest_common.output_path(), "static_instances_{}.txt".format(nbs_index))
    with open(instance_list_file, "w") as f:
        print("localhost\t%s\t%s" % (nbs_port, nbs_secure_port), file=f)

    discovery_config = TDiscoveryServiceConfig()
    discovery_config.InstanceListFile = instance_list_file

    cells_config_file = os.path.join(yatest_common.output_path(), "cells_config.json")
    cells_config = create_cells_config(args, cells_config_file, nbs_port, nbs_secure_port)

    kikimr_port = list(kikimr_cluster.nodes.values())[0].port
    nbs = LocalNbs(
        kikimr_port,
        configurator.domains_txt,
        server_app_config=server_app_config,
        enable_tls=True,
        load_configs_from_cms=True,
        discovery_config=discovery_config,
        nbs_secure_port=nbs_secure_port,
        nbs_port=nbs_port,
        kikimr_binary_path=kikimr_binary_path,
        nbs_binary_path=nbs_binary_path,
        use_ic_version_check=args.use_ic_version_check,
        config_sub_folder="nbs_configs_{}".format(nbs_index),
        cells_config=cells_config
        )

    nbs.setup_cms(kikimr_cluster.client)

    nbs.start()
    nbs_servers.append(nbs)

    append_recipe_err_files(ERR_LOG_FILE_NAMES_FILE, nbs.stderr_file_name)

    recipe_set_env("LOCAL_KIKIMR_KIKIMR_ROOT", configurator.domain_name, nbs_index)
    recipe_set_env("LOCAL_KIKIMR_KIKIMR_SERVER_PORT", str(kikimr_port), nbs_index)
    recipe_set_env("LOCAL_KIKIMR_SECURE_NBS_SERVER_PORT", str(nbs.nbs_secure_port), nbs_index)
    recipe_set_env("LOCAL_KIKIMR_INSECURE_NBS_SERVER_PORT", str(nbs.nbs_port), nbs_index)

    pid_file_name = PID_FILE_NAME.format(nbs_index)

    with open(pid_file_name, "w") as f:
        for nbs in nbs_servers:
            f.write(str(nbs.pid) + "\n")

    for nbs in nbs_servers:
        wait_for_nbs_server(nbs.nbs_port)


def stop(argv):
    args = pars_args(argv)

    pid_file_name = PID_FILE_NAME.format(args.nbs_index)
    with open(pid_file_name) as f:
        for line in f:
            pid = int(line.strip())
            os.kill(pid, signal.SIGTERM)
    errors = process_recipe_err_files(ERR_LOG_FILE_NAMES_FILE)
    if errors:
        raise RuntimeError("Errors during recipe execution:\n" + "\n".join(errors))


if __name__ == "__main__":
    declare_recipe(start, stop)
