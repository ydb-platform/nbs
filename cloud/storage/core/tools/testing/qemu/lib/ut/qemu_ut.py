from types import SimpleNamespace
from unittest import mock

import pytest

from cloud.storage.core.tools.testing.qemu.lib import qemu as qemu_module


def test_start_kills_qemu_when_qmp_initialization_fails():
    qemu = object.__new__(qemu_module.Qemu)
    qemu.qemu_bin = None
    qemu.enable_kvm = False
    qemu.rootfs = "/test/rootfs"
    qemu.backup_rootfs = False
    qemu.qmp_socket = "/tmp/test-qmp"

    process = mock.Mock()
    daemon = mock.Mock()
    daemon.daemon = SimpleNamespace(process=process)

    with mock.patch.object(
        qemu_module,
        "prepare_root_image",
        return_value="/test/rootfs",
    ), mock.patch.object(
        qemu,
        "_create_cmd",
        return_value=["qemu"],
    ), mock.patch.object(
        qemu_module,
        "daemon_log_files",
        return_value={},
    ), mock.patch.object(
        qemu_module.yatest.common,
        "output_path",
        return_value="/test/output",
    ), mock.patch.object(
        qemu_module.yatest.common,
        "work_path",
        return_value="/test/work",
    ), mock.patch.object(
        qemu_module,
        "Daemon",
        return_value=daemon,
    ), mock.patch.object(
        qemu_module,
        "QmpClient",
        side_effect=TimeoutError("QMP timed out"),
    ) as qmp_client:
        with pytest.raises(TimeoutError, match="QMP timed out"):
            qemu.start()

    daemon.start.assert_called_once_with()
    qmp_client.assert_called_once_with("/tmp/test-qmp", vm_proc=process)
    daemon.kill.assert_called_once_with()
    assert qemu.qemu_bin is None
