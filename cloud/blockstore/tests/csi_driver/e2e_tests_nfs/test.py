import subprocess


import cloud.blockstore.tests.csi_driver.lib.csi_runner as csi


def test_volume_lifecycle_local_fs_legacy():
    fs_name = "example-fs"
    fs_config = dict(fs_id=fs_name)
    fs_size = 1024**3
    pod_names = ["example-pod-1", "example-pod-2"]
    pod_ids = ["deadbeef1", "deadbeef2"]

    env, run = csi.init(vm_mode=True, external_fs_configs=[fs_config])
    try:
        env.csi.create_volume(name=fs_name, size=fs_size, is_nfs=True)
        env.csi.stage_volume(fs_name, "mount", is_nfs=True)
        for pod_name, pod_id in zip(pod_names, pod_ids):
            env.csi.publish_volume(
                pod_id=pod_id,
                volume_id=fs_name,
                pod_name=pod_name,
                access_type="mount",
                is_nfs=True,
            )
        for pod_name, pod_id in zip(pod_names, pod_ids):
            env.csi.unpublish_volume(
                pod_id=pod_id, volume_id=fs_name, access_type="mount"
            )

        env.csi.unstage_volume(volume_id=fs_name)

    except subprocess.CalledProcessError as e:
        csi.log_called_process_error(e)
        raise
    finally:
        csi.cleanup_after_test(
            env, volume_name=fs_name, access_type="mount", pods=pod_ids
        )


def test_volume_lifecycle_local_fs():
    fs_name = "example-fs"
    fs_config = dict(
        fs_id=fs_name,
        fs_type="external",
        fs_size_gb=100,
        fs_cloud_id="cloud",
        fs_folder_id="folder",
        fs_mount_cmd="/bin/bash",
        fs_mount_args=["-c", "echo hello world; echo LOCAL_FS_ID=$LOCAL_FS_ID; echo EXTERNAL_FS_ID=$EXTERNAL_FS_ID"],
        fs_umount_cmd="/bin/bash",
        fs_umount_args=["-c", "echo goodbye world; echo LOCAL_FS_ID=$LOCAL_FS_ID; echo EXTERNAL_FS_ID=$EXTERNAL_FS_ID"],
    )
    fs_size = 1024**3
    pod_names = ["example-pod-1", "example-pod-2"]
    pod_ids = ["deadbeef1", "deadbeef2"]

    env, run = csi.init(vm_mode=True, external_fs_configs=[fs_config])
    try:
        env.csi.create_volume(name=fs_name, size=fs_size, is_nfs=True)
        env.csi.stage_volume(fs_name, "mount", is_nfs=True)
        # repeated stage should be ok
        env.csi.stage_volume(fs_name, "mount", is_nfs=True)
        for pod_name, pod_id in zip(pod_names, pod_ids):
            env.csi.publish_volume(
                pod_id=pod_id,
                volume_id=fs_name,
                pod_name=pod_name,
                access_type="mount",
                is_nfs=True,
            )
        for pod_name, pod_id in zip(pod_names, pod_ids):
            env.csi.unpublish_volume(
                pod_id=pod_id, volume_id=fs_name, access_type="mount"
            )

        env.csi.unstage_volume(volume_id=fs_name)
        # repeated unstage of volume should be ok
        env.csi.unstage_volume(volume_id=fs_name)

    except subprocess.CalledProcessError as e:
        csi.log_called_process_error(e)
        raise
    finally:
        csi.cleanup_after_test(
            env, volume_name=fs_name, access_type="mount", pods=pod_ids
        )
