import yatest.common as common
import yatest.common.network as network

from cloud.blockstore.config.discovery_pb2 import TDiscoveryServiceConfig
from cloud.blockstore.config.server_pb2 import TServerAppConfig, TServerConfig, TKikimrServiceConfig
from cloud.blockstore.tests.python.lib.loadtest_env import LocalLoadTest
from cloud.blockstore.tests.python.lib.test_base import thread_count, run_test

import json
import os
import socket
from subprocess import call
import threading
import time


def run_async(job, stderr, stdout):
    def run():
        with open(os.path.join(common.output_path(), stderr), "w") as err:
            with open(os.path.join(common.output_path(), stdout), "w") as out:
                call(job, stderr=err, stdout=out)
    t = threading.Thread(target=run)
    t.setDaemon(True)
    t.start()


def tcp_check(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        location = ("localhost", port)
        result_of_check = s.connect_ex(location)

        return result_of_check == 0

    finally:
        s.close()


def test_load():
    port_manager = network.PortManager()
    nbs_ports = [port_manager.get_port() for i in range(10)]
    conductor_port = port_manager.get_port()

    fake_nbs_path = common.binary_path(
        "cloud/blockstore/tools/testing/fake-nbs/blockstore-fake-nbs")
    fake_conductor = common.binary_path(
        "cloud/blockstore/tools/testing/fake-conductor/blockstore-fake-conductor")

    good_ports = nbs_ports[len(nbs_ports) // 2:]

    for nbs_port in good_ports:
        run_async(
            [fake_nbs_path, "--port", str(nbs_port)],
            "fake-nbs-%s.out" % nbs_port,
            "fake-nbs-%s.err" % nbs_port
        )

    run_async(
        [
            fake_conductor,
            "--port", str(conductor_port),
            "--instance", "group1/localhost",
        ],
        "fake-conductor-%s.out" % nbs_port,
        "fake-conductor-%s.err" % nbs_port
    )

    unchecked = list(good_ports)
    unchecked.append(conductor_port)

    attempts = 0
    while True:
        failed = []
        for port in unchecked:
            if not tcp_check(port):
                failed.append(port)

        if len(failed) > 0:
            time.sleep(1)
            attempts += 1

            if attempts == 10:
                raise Exception("failed to ping ports: %s" % failed)
        else:
            break

        unchecked = failed

    instance_list_file_path = os.path.join(common.output_path(), "static_instances.txt")
    with open(instance_list_file_path, "w") as f:
        for nbs_port in nbs_ports[1:]:
            print("localhost\t%s\t1" % (nbs_port), file=f)

    discovery_config = TDiscoveryServiceConfig()
    discovery_config.ConductorApiUrl = "http://localhost:%s/api" % conductor_port
    discovery_config.InstanceListFile = instance_list_file_path
    discovery_config.ConductorRequestInterval = 1000
    discovery_config.LocalFilesReloadInterval = 1000
    discovery_config.HealthCheckInterval = 500
    discovery_config.ConductorGroups.append("group1")
    discovery_config.ConductorInstancePort = nbs_ports[0]
    discovery_config.ConductorSecureInstancePort = 2

    server = TServerAppConfig()
    server.ServerConfig.CopyFrom(TServerConfig())
    server.ServerConfig.ThreadsCount = thread_count()
    server.ServerConfig.StrictContractValidation = False
    server.KikimrServiceConfig.CopyFrom(TKikimrServiceConfig())

    env = LocalLoadTest(
        "",
        server_app_config=server,
        discovery_config=discovery_config,
        use_in_memory_pdisks=True,
    )

    try:
        ret = run_test(
            "load",
            common.source_path(
                "cloud/blockstore/tests/loadtest/local-discovery/local.txt"),
            env.nbs_port,
            env.mon_port,
            env_processes=[env.nbs],
        )

        results_path = os.path.join(common.output_path(), "results.txt")

        def is_good(port):
            return port in good_ports

        with open(results_path, "r") as f:
            for line in f:
                data = json.loads(line)
                ports = [x['Port'] for x in data['Instances']]
                for port in ports:
                    assert is_good(port)
    finally:
        env.tear_down()

    return ret
