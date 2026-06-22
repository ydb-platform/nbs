import pytest
import subprocess
import stat
import os
import tempfile

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


def get_target_path(pod_id: str, volume_name: str, access_type: str) -> Path:
    if access_type == "block":
        return (
            Path("/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/publish")
            / pod_id
            / volume_name
        )

    return (
        Path("/var/lib/kubelet/pods")
        / pod_id
        / "volumes/kubernetes.io~csi"
        / volume_name
        / "mount"
    )


def assert_endpoints(run, volume_name: str, total: int, read_only: int, remote: int):
    endpoints = run("listendpoints", "--proto").stdout
    assert total == endpoints.count(f'DiskId: "{volume_name}"')
    assert read_only == endpoints.count("VolumeAccessMode: VOLUME_ACCESS_READ_ONLY")
    assert remote == endpoints.count("VolumeMountMode: VOLUME_MOUNT_REMOTE")


def test_publish_distinguishes_remote_mount_access_modes():
    env, run = csi.init(vm_mode=True)
    volume_name = "example-disk"
    volume_size = 1024 ** 3
    pod_name = "example-pod"
    access_type = "mount"
    io_dir = tempfile.TemporaryDirectory()
    io_path = Path(io_dir.name)
    published_pvcs = []
    staged_pvcs = []

    def run_checked(*args, **kwargs):
        result = run(*args, **kwargs)
        assert result.returncode == 0, (
            f"command failed: {' '.join(args)}\n"
            f"stdout: {result.stdout}\n"
            f"stderr: {result.stderr}")
        return result

    def make_pvc(
            name: str,
            pod_id: str,
            access_mode: str,
            readonly: bool = False):
        return {
            "name": name,
            "pod_id": pod_id,
            "volume_id": f"{volume_name}#{name}",
            "instance_id": name,
            "access_mode": access_mode,
            "readonly": readonly,
            "target_path": str(get_target_path(pod_id, name, access_type)),
            "staging_target_path": str(
                Path("/var/lib/kubelet/plugins/kubernetes.io/csi/nbs.csi.nebius.ai")
                / name
                / "globalmount"),
        }

    def target_path(pvc) -> Path:
        return Path(pvc["target_path"])

    def stage(pvc):
        env.csi.stage_volume(
            pvc["volume_id"],
            access_type,
            instance_id=pvc["instance_id"],
            staging_target_path=pvc["staging_target_path"],
            access_mode=pvc["access_mode"])
        staged_pvcs.append(pvc)

    def unstage(pvc):
        env.csi.unstage_volume(
            pvc["volume_id"],
            staging_target_path=pvc["staging_target_path"])
        staged_pvcs.remove(pvc)

    def publish(pvc):
        env.csi.publish_volume(
            pod_id=pvc["pod_id"],
            volume_id=pvc["volume_id"],
            pod_name=pod_name,
            access_type=access_type,
            instance_id=pvc["instance_id"],
            staging_target_path=pvc["staging_target_path"],
            target_path=pvc["target_path"],
            readonly=pvc["readonly"],
            access_mode=pvc["access_mode"])
        published_pvcs.append(pvc)

    def unpublish(pvc):
        env.csi.unpublish_volume(
            pvc["pod_id"],
            pvc["volume_id"],
            access_type,
            target_path=pvc["target_path"])
        published_pvcs.remove(pvc)

    def client_config_path(pvc) -> Path:
        path = io_path / f"{pvc['name']}.client-config.txt"
        client_id = f"nbs.csi.nebius.ai-localhost-{pvc['instance_id']}"
        path.write_text(
            "ClientConfig {\n"
            "  Host: \"localhost\"\n"
            f"  InsecurePort: {env.nbs_port}\n"
            f"  InstanceId: \"{pvc['instance_id']}\"\n"
            f"  ClientId: \"{client_id}\"\n"
            "}\n")
        return path

    def assert_shared_disk_data_visible(writer, readers):
        payload = b"written by multi-node-single-writer pvc\n".ljust(4096, b"\0")
        input_path = io_path / "writer.blocks"
        input_path.write_bytes(payload)

        # VM-mode publish exposes vhost sockets; validate shared data at the NBS disk level.
        run_checked(
            "writeblocks",
            "--disk-id",
            volume_name,
            "--start-index",
            "0",
            "--input",
            str(input_path),
            config_path=client_config_path(writer))

        for reader in readers:
            output_path = io_path / f"{reader['name']}.blocks"
            run_checked(
                "readblocks",
                "--disk-id",
                volume_name,
                "--start-index",
                "0",
                "--blocks-count",
                "1",
                "--output",
                str(output_path),
                config_path=client_config_path(reader))
            assert payload == output_path.read_bytes()

    try:
        env.csi.create_volume(name=volume_name, size=volume_size)

        single_reader = make_pvc(
            "single-reader-pvc",
            "single-reader",
            "single-node-reader-only")
        stage(single_reader)

        assert_endpoints(
            run,
            volume_name,
            total=1,
            read_only=1,
            remote=0)

        publish(single_reader)
        assert "ro" == get_access_mode(str(target_path(single_reader)))
        unpublish(single_reader)
        unstage(single_reader)

        pvcs = [
            make_pvc("writer-pvc", "writer", "multi-node-single-writer"),
            make_pvc("reader1-pvc", "reader1", "multi-node-reader-only"),
            make_pvc("reader2-pvc", "reader2", "multi-node-reader-only"),
        ]

        for pvc in pvcs:
            stage(pvc)

        assert_endpoints(
            run,
            volume_name,
            total=3,
            read_only=2,
            remote=3)

        for pvc in pvcs:
            publish(pvc)

        assert "rw" == get_access_mode(str(target_path(pvcs[0])))
        for pvc in pvcs[1:]:
            assert "ro" == get_access_mode(str(target_path(pvc)))

        assert_shared_disk_data_visible(pvcs[0], pvcs[1:])

        for pvc in reversed(pvcs):
            unpublish(pvc)
        for pvc in reversed(pvcs):
            unstage(pvc)

    except subprocess.CalledProcessError as e:
        csi.log_called_process_error(e)
        raise
    finally:
        for pvc in reversed(published_pvcs):
            with csi.called_process_error_logged():
                env.csi.unpublish_volume(
                    pvc["pod_id"],
                    pvc["volume_id"],
                    access_type,
                    target_path=pvc["target_path"])
        for pvc in reversed(staged_pvcs):
            with csi.called_process_error_logged():
                env.csi.unstage_volume(
                    pvc["volume_id"],
                    staging_target_path=pvc["staging_target_path"])
        with csi.called_process_error_logged():
            env.csi.delete_volume(volume_name)
        env.tear_down()
        io_dir.cleanup()


def test_mount_volume_group():
    # Scenario
    # 1. create volume and stage it
    # 2. verify that the setgid bit is not set
    # 3. create directory and file in the staging directory
    # 4. create new group with specified GID
    # 5. publish volume with mount volume group GID
    # 6. verify that the setgid bit is set
    # 7. check that mounted dir and existing files have specified GID
    # 8. create new directory and file
    # 9. check that new directory and file have specified GID
    # 10. unpublish volume
    # 11. verify that the setgid bit is set
    # 12. create new file in staging directory and change ownership
    # 13. publish volume with mount volume group GID
    # 14. Verify that the new file doesn't have the specified GID.
    # 15. Unpublish, unstage volume and stage it again
    # 16. verify that the setgid bit is set
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

        assert os.stat(stage_path).st_mode & stat.S_ISGID == 0

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
        assert os.stat(mount_path).st_mode & stat.S_ISGID != 0

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
        assert os.stat(mount_path).st_mode & stat.S_ISGID != 0

        test_file3 = test_dir1 / "testfile3"
        assert gid != test_file3.stat().st_gid

        env.csi.unpublish_volume(pod_id, volume_name, access_type)
        env.csi.unstage_volume(volume_name)
        env.csi.stage_volume(volume_name, access_type)
        assert os.stat(stage_path).st_mode & stat.S_ISGID != 0

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


def test_stage_twice_with_different_parameters():
    env, run = csi.init(vm_mode=True)
    try:
        volume_name = "example-disk"
        volume_size = 1024 ** 3
        access_type = "mount"
        env.csi.create_volume(name=volume_name, size=volume_size)
        env.csi.stage_volume(volume_name, access_type, vhost_request_queues_count=4)
        env.csi.stage_volume(volume_name, access_type, vhost_request_queues_count=8)
    except subprocess.CalledProcessError as e:
        csi.log_called_process_error(e)
        raise
    finally:
        csi.cleanup_after_test(env, volume_name, access_type)
