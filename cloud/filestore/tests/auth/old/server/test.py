import json

from cloud.filestore.tests.auth.lib import TestFixture


def test_auth_unauthorized():
    fixture = TestFixture()
    token = "test_auth_token"
    client = fixture.get_client(token)
    fixture.access_service.authenticate(token)
    result = client.create(
        "test_auth_unauthorized_fs",
        "some_cloud",
        fixture.folder_id,
        return_stdout=False,
    )
    assert result.returncode != 0
    assert json.loads(result.stdout.decode())["Error"]["CodeString"] == "E_UNAUTHORIZED"
