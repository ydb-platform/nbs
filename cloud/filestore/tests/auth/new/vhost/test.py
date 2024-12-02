import os

from cloud.filestore.tests.auth.lib import TestFixture, log_result


def test_unix_socket_does_not_require_auth():
    fixture = TestFixture()
    client = fixture.get_client("some-token", unix_socket=os.getenv("NFS_VHOST_UNIX_SOCKET_PATH"))
    result = client.list_endpoints()
    log_result("test_unix_socket_does_not_require_auth", result)
    assert result.returncode == 0
