import pytest
import subprocess
import yaml

from pathlib import Path

import yatest.common as common
import cloud.blockstore.tests.csi_driver.lib.csi_runner as csi


@pytest.mark.parametrize('mount_path,volume_access_type,vm_mode,skip_tests',
                         [("/var/lib/kubelet/pods/123/volumes/kubernetes.io~csi/456/mount", "mount", False, []),
                          ("/var/lib/kubelet/plugins/kubernetes.io/csi/volumeDevices/publish/123/456", "block", False, []),
                          ("/var/lib/kubelet/pods/123/volumes/kubernetes.io~csi/456/mount", "mount", True,
                           ["should work if node-expand is called after node-publish"])])
def test_csi_sanity_nbs_backend(mount_path, volume_access_type, vm_mode, skip_tests):
    env, run = csi.init(vm_mode)
    backend = "nbs"

    try:
        CSI_SANITY_BINARY_PATH = common.binary_path("cloud/blockstore/tools/testing/csi-sanity/bin/csi-sanity")
        mount_dir = Path(mount_path)
        mount_dir.parent.mkdir(parents=True, exist_ok=True)

        params_file = Path("params.yaml")
        params_file.write_text(yaml.safe_dump(
            {
                "backend": backend,
                "instanceId": "test-instance-id",
            }
        ))

        args = [CSI_SANITY_BINARY_PATH,
                "-csi.endpoint",
                env.csi._endpoint,
                "--csi.mountdir",
                mount_dir,
                "-csi.testvolumeparameters",
                params_file,
                "-csi.testvolumeaccesstype",
                volume_access_type,
                "--ginkgo.skip",
                '|'.join(skip_tests)]
        subprocess.run(
            args,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as e:
        csi.log_called_process_error(e)
        raise
    finally:
        csi.cleanup_after_test(env)
