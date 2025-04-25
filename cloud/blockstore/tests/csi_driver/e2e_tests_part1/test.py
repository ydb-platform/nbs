import logging
import pytest
import subprocess

from pathlib import Path

import cloud.blockstore.tests.csi_driver.lib.csi_runner as csi


def test_nbs_csi_driver_mounted_disk_protected_from_deletion():
    env, run = csi.init(retry_timeout_ms=5000)
    try:
        volume_name = "example-disk"
        volume_size = 10 * 1024 ** 3
        pod_name = "example-pod"
        pod_id = "deadbeef"
        access_type = "mount"
        env.csi.create_volume(name=volume_name, size=volume_size)
        env.csi.stage_volume(volume_name, access_type)
        env.csi.publish_volume(pod_id, volume_name, pod_name, access_type)
        result = run(
            "destroyvolume",
            "--disk-id",
            volume_name,
            input=volume_name,
            code=1,
        )
        logging.info("Stdout: %s", result.stdout)
        logging.info("Stderr: %s", result.stderr)
        if result.returncode != 1:
            raise AssertionError("Destroyvolume must return exit code 1")
        assert "E_REJECTED" in result.stdout
    except subprocess.CalledProcessError as e:
        csi.log_called_process_error(e)
        raise
    finally:
        csi.cleanup_after_test(env, volume_name, access_type, [pod_id])


def test_nbs_csi_driver_volume_stat():
    # Scenario
    # 1. create volume and publish volume
    # 2. get volume stats and validate output
    # 3. create two files in the mounted directory
    # 4. get volume stats again and validate output
    # 5. check that the difference between used/available bytes is 2 block sizes
    # 6. check that the difference between used/available inodes is 2
    env, run = csi.init()
    try:
        volume_name = "example-disk"
        volume_size = 1024 ** 3
        pod_name = "example-pod"
        pod_id = "deadbeef"
        access_type = "mount"
        env.csi.create_volume(name=volume_name, size=volume_size)
        env.csi.stage_volume(volume_name, access_type)
        env.csi.publish_volume(pod_id, volume_name, pod_name, access_type)
        stats1 = env.csi.volumestats(pod_id, volume_name)

        assert "usage" in stats1
        usage_array1 = stats1["usage"]
        assert 2 == len(usage_array1)
        for usage in usage_array1:
            usage = usage_array1[0]
            assert {"unit", "total", "available", "used"} == usage.keys()
            assert 0 != usage["total"]
            assert 0 != usage["available"]
            assert 0 != usage["used"]
            assert usage["total"] >= usage["available"] + usage["used"]

        mount_path = Path("/var/lib/kubelet/pods") / pod_id / "volumes/kubernetes.io~csi" / volume_name / "mount"
        (mount_path / "test1.file").write_bytes(b"\0")
        (mount_path / "test2.file").write_bytes(b"\0")

        stats2 = env.csi.volumestats(pod_id, volume_name)
        assert "usage" in stats2
        usage_array2 = stats2["usage"]
        assert 2 == len(usage_array2)
        for usage in usage_array2:
            usage = usage_array2[0]
            assert {"unit", "total", "available", "used"} == usage.keys()
            assert 0 != usage["total"]
            assert 0 != usage["available"]
            assert 0 != usage["used"]
            assert usage["total"] >= usage["available"] + usage["used"]

        bytesUsage1 = usage_array1[0]
        bytesUsage2 = usage_array2[0]
        assert 4096 * 2 == bytesUsage1["available"] - bytesUsage2["available"]
        assert 4096 * 2 == bytesUsage2["used"] - bytesUsage1["used"]

        nodesUsage1 = usage_array1[1]
        nodesUsage2 = usage_array2[1]
        assert 2 == nodesUsage1["available"] - nodesUsage2["available"]
        assert 2 == nodesUsage2["used"] - nodesUsage1["used"]
    except subprocess.CalledProcessError as e:
        csi.log_called_process_error(e)
        raise
    finally:
        csi.cleanup_after_test(env, volume_name, access_type, [pod_id])


@pytest.mark.parametrize('fs_type', ["ext4", "xfs"])
def test_node_volume_expand(fs_type):
    env, run = csi.init()
    try:
        volume_name = "example-disk"
        volume_size = 1024 ** 3
        pod_name = "example-pod"
        pod_id = "deadbeef"
        access_type = "mount"
        env.csi.create_volume(name=volume_name, size=volume_size)

        env.csi.stage_volume(volume_name, access_type)
        env.csi.publish_volume(pod_id, volume_name, pod_name, access_type, fs_type)

        new_volume_size = 2 * volume_size
        env.csi.expand_volume(pod_id, volume_name, new_volume_size, access_type)

        stats = env.csi.volumestats(pod_id, volume_name)
        assert "usage" in stats
        usage_array = stats["usage"]
        assert 2 == len(usage_array)
        bytes_usage = usage_array[0]
        assert "total" in bytes_usage
        # approximate check that total space is around 2GB
        assert bytes_usage["total"] // 1000 ** 3 == 2

        # check that expand_volume is idempotent method
        env.csi.expand_volume(pod_id, volume_name, new_volume_size, access_type)
    except subprocess.CalledProcessError as e:
        csi.log_called_process_error(e)
        raise
    finally:
        csi.cleanup_after_test(env, volume_name, access_type, [pod_id])


@pytest.mark.parametrize('access_type,vm_mode', [("mount", True), ("mount", False), ("block", False)])
def test_publish_volume_twice_on_the_same_node(access_type, vm_mode):
    env, run = csi.init(vm_mode=vm_mode)
    try:
        volume_name = "example-disk"
        volume_size = 1024 ** 3
        pod_name1 = "example-pod-1"
        pod_name2 = "example-pod-2"
        pod_id1 = "deadbeef1"
        pod_id2 = "deadbeef2"
        env.csi.create_volume(name=volume_name, size=volume_size)
        env.csi.stage_volume(volume_name, access_type)
        env.csi.publish_volume(pod_id1, volume_name, pod_name1, access_type)
        env.csi.publish_volume(pod_id2, volume_name, pod_name2, access_type)
    except subprocess.CalledProcessError as e:
        csi.log_called_process_error(e)
        raise
    finally:
        csi.cleanup_after_test(env, volume_name, access_type, [pod_id1, pod_id2])


@pytest.mark.parametrize('access_type', ["mount", "block"])
def test_kubelet_restart(access_type):
    env, run = csi.init()
    try:
        volume_name = "example-disk"
        volume_size = 1024 ** 3
        pod_name1 = "example-pod-1"
        pod_id1 = "deadbeef1"
        endpoint_dir = Path(env.csi._sockets_dir) / "example-disk"
        env.csi.create_volume(name=volume_name, size=volume_size)
        env.csi.stage_volume(volume_name, access_type)
        assert endpoint_dir.exists()
        env.csi.publish_volume(pod_id1, volume_name, pod_name1, access_type)
        # run stage/publish again to simulate kubelet restart
        env.csi.stage_volume(volume_name, access_type)
        env.csi.publish_volume(pod_id1, volume_name, pod_name1, access_type)
        env.csi.unpublish_volume(pod_id1, volume_name, access_type)
        env.csi.unstage_volume(volume_name)
        assert not endpoint_dir.exists()
    except subprocess.CalledProcessError as e:
        csi.log_called_process_error(e)
        raise
    finally:
        csi.cleanup_after_test(env, volume_name, access_type, [pod_id1])
