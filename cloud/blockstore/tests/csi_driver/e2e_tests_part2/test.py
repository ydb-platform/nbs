import pytest
import subprocess

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


@pytest.mark.parametrize('mount_path,access_type,vm_mode',
                         [("/var/lib/kubelet/pods/123/volumes/kubernetes.io~csi/example-disk/mount", "mount", False),
                          ("/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/publish/123/example-disk", "block", False),
                          ("/var/lib/kubelet/pods/123/volumes/kubernetes.io~csi/example-disk/mount", "mount", True)])
def test_readonly_volume(mount_path, access_type, vm_mode):
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

    except subprocess.CalledProcessError as e:
        csi.log_called_process_error(e)
        raise
    finally:
        csi.cleanup_after_test(env, volume_name, access_type, [pod_id])
