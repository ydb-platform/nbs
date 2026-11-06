import json
import logging
import os
import threading

import yatest.common as common
from yatest.common.network import PortManager

from cloud.storage.core.tools.testing.qemu.lib.common import (
    env_with_guest_index,
    SshToGuest,
)

logger = logging.getLogger(__name__)

# The guests reach the host's loopback via the slirp host address.
HUB_HOST_FROM_GUEST = "10.0.2.2"

FMDTEST_PATH = "cloud/filestore/tools/testing/fmdtest/bin/fmdtest"


def _run_instance(ssh, command, results, index):
    try:
        results[index] = ssh(command)
    except Exception as e:
        logger.exception("instance %d failed", index)
        results[index] = e


def run_multi_client_test(
        test_name,
        instance_count,
        rounds,
        files_per_round,
        sync_timeout="5m"):
    # Runs the multi-client mode of fmdtest in each guest. The instances are
    # synchronized via the hub - another fmdtest process run on the host. Each
    # instance modifies its own directory in the shared filesystem and checks
    # the directories of the other instances after each round.

    fmdtest_bin = common.binary_path(FMDTEST_PATH)
    ssh_key = os.getenv("QEMU_SSH_KEY")
    mount_dir = os.getenv("NFS_MOUNT_PATH")
    test_dir = os.path.join(mount_dir, test_name + "_wd")

    with PortManager() as pm:
        port = pm.get_port()
        hub_log_path = os.path.join(
            common.output_path(),
            test_name + "_hub.log")
        with open(hub_log_path, "w") as hub_log:
            hub = common.execute(
                [
                    fmdtest_bin,
                    "--mode", "sync-hub",
                    "--sync-port", str(port),
                    "--instance-count", str(instance_count),
                    "--sync-timeout", sync_timeout,
                ],
                wait=False,
                stdout=hub_log,
                stderr=hub_log)

            try:
                guests = []
                threads = []
                results = [None] * instance_count
                for i in range(instance_count):
                    ssh_port = int(os.getenv(
                        env_with_guest_index("QEMU_FORWARDING_PORT", i)))
                    ssh = SshToGuest(user="qemu", port=ssh_port, key=ssh_key)
                    guests.append(ssh)

                    command = (
                        f"{fmdtest_bin} --mode multi-client"
                        f" --test-dir {test_dir}"
                        f" --instance-id {i}"
                        f" --instance-count {instance_count}"
                        f" --sync-address {HUB_HOST_FROM_GUEST}:{port}"
                        f" --sync-timeout {sync_timeout}"
                        f" --rounds {rounds}"
                        f" --files-per-round {files_per_round}"
                        f" --report-path report_{i}.json")
                    t = threading.Thread(
                        target=_run_instance,
                        args=(ssh, command, results, i))
                    t.start()
                    threads.append(t)

                for t in threads:
                    t.join()

                hub.wait(check_exit_code=False)
            finally:
                if hub.running:
                    hub.kill()

    reports = []
    for i, ssh in enumerate(guests):
        report = json.loads(
            ssh(f"cat report_{i}.json").stdout.decode("utf8"))
        reports.append(report)
        report_path = os.path.join(
            common.output_path(),
            f"{test_name}_report_{i}.json")
        with open(report_path, "w") as f:
            f.write(json.dumps(report, indent=4))

    for i, result in enumerate(results):
        assert not isinstance(result, Exception), \
            f"instance {i} failed: {result}, report: {reports[i]}"

    assert hub.exit_code == 0, f"hub failed, see {hub_log_path}"

    for report in reports:
        assert report["fatalError"] == "", report
        assert report["errors"] == 0, report
        assert report["rounds"] == rounds, report
