import pytest
import subprocess
import os

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


def test_mount_volume_group():
    # Scenario
    # 1. create volume and stage it
    # 2. create directory and file in the staging directory
    # 4. create new group with specified GID
    # 5. publish volume with mount volume group GID
    # 6. check that mounted dir and existing files have specified GID
    # 7. create new directory and file
    # 8. check that new directory and file have specified GID
    # 9. unpublish volume
    # 10. create new file in staging directory and change ownership
    # 11. publish volume with mount volume group GID
    # 12. Verify that the new file doesn't have the specified GID.
    # The change won't take effect because the GID of the mount directory
    # matches the GID of the volume group.

    stage_path = Path("/var/lib/kubelet/plugins/kubernetes.io/csi/nbs.csi.nebius.ai/a/globalmount")
    env, run = csi.init()
    try:
        volume_name = "example-disk"
        volume_size = 1024 ** 3
        pod_name = "example-pod"
        pod_id = "deadbeef1"
        access_type = "mount"
        env.csi.create_volume(name=volume_name, size=volume_size)
        env.csi.stage_volume(volume_name, access_type)

        stage_test_dir1 = stage_path / "testdir1"
        stage_test_dir1.mkdir()
        stage_test_file1 = stage_test_dir1 / "testfile1"
        stage_test_file1.touch()

        gid = 1013
        result = subprocess.run(
            ["groupadd", "-g", str(gid), "test_group_" + str(gid)],
            capture_output=True,
        )
        assert result.returncode == 0

        env.csi.publish_volume(
            pod_id,
            volume_name,
            pod_name,
            access_type,
            volume_mount_group=str(gid)
        )

        mount_path = Path("/var/lib/kubelet/pods") / pod_id / "volumes/kubernetes.io~csi" / volume_name / "mount"
        test_dir1 = mount_path / "testdir1"
        test_file1 = test_dir1 / "testfile1"

        assert gid == mount_path.stat().st_gid
        assert gid == test_dir1.stat().st_gid
        assert gid == test_file1.stat().st_gid

        test_file2 = mount_path / "testfile2"
        test_file2.touch()
        assert gid == test_file2.stat().st_gid

        test_dir2 = mount_path / "testdir2"
        test_dir2.mkdir()
        assert gid == test_dir2.stat().st_gid

        env.csi.unpublish_volume(pod_id, volume_name, access_type)

        stage_test_file3 = stage_test_dir1 / "testfile3"
        stage_test_file3.touch()
        os.chown(stage_test_file3, os.getuid(), os.getgid())
        assert gid != stage_test_file3.stat().st_gid

        env.csi.publish_volume(
            pod_id,
            volume_name,
            pod_name,
            access_type,
            volume_mount_group=str(gid)
        )

        test_file3 = test_dir1 / "testfile3"
        assert gid != test_file3.stat().st_gid

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


@pytest.mark.skip(reason="issue-3635")
def test_publish_volume_must_fail_after_fs_error():
    env, run = csi.init()
    try:
        volume_name = "example-disk"
        volume_size = 1024 ** 3
        pod_name = "example-pod"
        pod_id = "deadbeef1"
        access_type = "mount"
        env.csi.create_volume(name=volume_name, size=volume_size)
        env.csi.stage_volume(volume_name, access_type)
        env.csi.publish_volume(pod_id, volume_name, pod_name, access_type)

        with open('/sys/fs/ext4/nbd0/trigger_fs_error', 'w') as f:
            f.write("test error")

        env.csi.unpublish_volume(pod_id, volume_name, access_type)

        stage_path = "/var/lib/kubelet/plugins/kubernetes.io/csi/nbs.csi.nebius.ai/a/globalmount"
        assert "ro" == get_access_mode(stage_path)

        try:
            env.csi.publish_volume(pod_id, volume_name, pod_name, access_type)
            assert False
        except subprocess.CalledProcessError:
            pass

    except subprocess.CalledProcessError as e:
        csi.log_called_process_error(e)
        raise
    finally:
        csi.cleanup_after_test(env, volume_name, access_type, [pod_id])
