import pytest
import subprocess

from pathlib import Path

import cloud.blockstore.tests.csi_driver.lib.csi_runner as csi


def get_access_mode(mount_point: str) -> str:
    with open('/proc/mounts', 'r') as f:
        for line in f.readlines():
            mount_data = line.split()
            if len(mount_data) <= 3 or mount_data[1] != mount_point:
                continue
            parameters = mount_data[3].split(',')
            if len(parameters) == 0:
                continue
            return parameters[0]
    return ""


@pytest.mark.parametrize('mount_path,access_type,vm_mode,gid',
                         [("/var/lib/kubelet/pods/123/volumes/kubernetes.io~csi/example-disk/mount", "mount", False, "1010"),
                          ("/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/publish/123/example-disk", "block", False, "1011"),
                          ("/var/lib/kubelet/pods/123/volumes/kubernetes.io~csi/example-disk/mount", "mount", True, "1012")])
def test_readonly_volume(mount_path, access_type, vm_mode, gid):
    env, run = csi.init(vm_mode)
    try:
        volume_name = "example-disk"
        volume_size = 1024 ** 3
        pod_name = "456"
        pod_id = "123"
        env.csi.create_volume(name=volume_name, size=volume_size)
        env.csi.stage_volume(volume_name, access_type)
        env.csi.publish_volume(pod_id, volume_name, pod_name, access_type, readonly=True)
        # check that publishing read only volume is idempotent
        env.csi.publish_volume(pod_id, volume_name, pod_name, access_type, readonly=True)
        assert "ro" == get_access_mode(mount_path)

        env.csi.unpublish_volume(pod_id, volume_name, access_type)
        result = subprocess.run(
            ["groupadd", "-g", gid, "test_group_" + gid],
            capture_output=True,
        )
        assert result.returncode == 0

        env.csi.publish_volume(
            pod_id,
            volume_name,
            pod_name,
            access_type,
            readonly=True,
            volume_mount_group=gid
        )
        assert "ro" == get_access_mode(mount_path)

    except subprocess.CalledProcessError as e:
        csi.log_called_process_error(e)
        raise
    finally:
        csi.cleanup_after_test(env, volume_name, access_type, [pod_id])


def test_node_volume_expand_vm_mode():
    env, run = csi.init(vm_mode=True)
    try:
        volume_name = "example-disk"
        block_size = 4096
        blocks_count = 1000
        volume_size = blocks_count * block_size
        pod_name = "example-pod"
        pod_id = "deadbeef"
        access_type = "mount"
        env.csi.create_volume(name=volume_name, size=volume_size)
        env.csi.stage_volume(volume_name, access_type)
        env.csi.publish_volume(pod_id, volume_name, pod_name, access_type)

        new_volume_size = 2 * volume_size
        result = run(
            "resizevolume",
            "--disk-id",
            volume_name,
            "--blocks-count",
            str(blocks_count * 2)
        )
        assert result.returncode == 0

        try:
            # expand volume to wrong size must fail
            env.csi.expand_volume(pod_id, volume_name, 2 * new_volume_size, access_type)
            assert False
        except subprocess.CalledProcessError:
            pass

        env.csi.expand_volume(pod_id, volume_name, new_volume_size, access_type)
        # check that expand_volume is idempotent method
        env.csi.expand_volume(pod_id, volume_name, new_volume_size, access_type)
    except subprocess.CalledProcessError as e:
        csi.log_called_process_error(e)
        raise
    finally:
        csi.cleanup_after_test(env, volume_name, access_type, [pod_id])
