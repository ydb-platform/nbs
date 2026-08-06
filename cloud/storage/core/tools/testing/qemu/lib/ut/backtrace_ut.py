from unittest import mock

from cloud.storage.core.tools.testing.qemu.lib import backtrace


def test_process_coredumps_skips_an_uninstalled_guest_script():
    ssh = mock.Mock()

    backtrace.process_coredumps(ssh)

    command = ssh.call_args.args[0]
    assert "if sudo test -x /process_coredumps.sh" in command
    assert "sudo /process_coredumps.sh" in command
    assert "is not installed" in command
