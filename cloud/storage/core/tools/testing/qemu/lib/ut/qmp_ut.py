from unittest import mock

import pytest

from cloud.storage.core.tools.testing.qemu.lib import qmp


def test_init_closes_socket_when_capability_initialization_fails():
    sock = mock.Mock()

    with mock.patch.object(qmp.socket, "socket", return_value=sock), \
            mock.patch.object(
                qmp.QmpClient,
                "command",
                side_effect=TimeoutError("QMP timed out"),
            ):
        with pytest.raises(TimeoutError, match="QMP timed out"):
            qmp.QmpClient("/tmp/test-qmp")

    sock.connect.assert_called_once_with("/tmp/test-qmp")
    sock.close.assert_called_once_with()
