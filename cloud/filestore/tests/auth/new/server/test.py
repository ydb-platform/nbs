import json

from cloud.filestore.tests.auth.lib import TestFixture, log_result


def test_new_auth_authorization_ok():
    fixture = TestFixture()
    token = "test_auth_token"
    client = fixture.get_client(token)
    fixture.access_service.create_account(
        "authorized_user_1",
        token,
        is_unknown_subject=False,
        permissions=[
            {"permission": "nbsInternal.disks.create", "resource": fixture.folder_id},
        ],
    )
    result = client.create(
        "test_new_auth_authorization_ok",
        "some_cloud",
        fixture.folder_id,
        return_stdout=False,
    )
    log_result("test_new_auth_authorization_ok", result)
    assert result.returncode == 0

# TODO: enable when unix sockets without auth are supported
# def test_unix_socket_does_not_require_auth():
#     fixture = TestFixture()
#     client = fixture.get_client("some-token", use_unix_socket=True)
#     result = client.create(
#         "test_unix_socket_does_not_require_auth",
#         "some_cloud",
#         fixture.folder_id,
#         return_stdout=False,
#     )
#     log_result("test_unix_socket_does_not_require_auth", result)
#     assert result.returncode != 0


def test_new_auth_unauthorized():
    fixture = TestFixture()
    token = "test_auth_token"
    client = fixture.get_client(token)
    fixture.access_service.create_account(
        "test_user",
        token,
        is_unknown_subject=False,
        permissions=[
            {"permission": "nbsInternal.disks.create", "resource": "some_other_folder"},
        ],
    )
    result = client.create(
        "test_new_auth_unauthorized",
        "some_cloud",
        fixture.folder_id,
        return_stdout=False,
    )
    log_result("test_new_auth_unauthorized", result)
    assert result.returncode != 0
    assert json.loads(result.stdout.decode())["Error"]["CodeString"] == "E_UNAUTHORIZED"


def test_new_auth_unauthenticated():
    fixture = TestFixture()
    client = fixture.get_client("some_other_token")
    result = client.create(
        "test_new_auth_unauthenticated_fs",
        "some_cloud",
        fixture.folder_id,
        return_stdout=False,
    )
    log_result("test_new_auth_unauthenticated", result)
    assert result.returncode != 0
    assert json.loads(result.stdout.decode())["Error"]["CodeString"] == "E_UNAUTHORIZED"


def test_new_auth_unknown_subject():
    fixture = TestFixture()
    token = "test_token"
    client = fixture.get_client(token)
    fixture.access_service.create_account(
        "test_user",
        token,
        is_unknown_subject=True,
        permissions=[
            {"permission": "nbsInternal.disks.create", "resource": fixture.folder_id},
        ],
    )
    result = client.create(
        "test_new_auth_unknown_subject_fs",
        "some_cloud",
        fixture.folder_id,
        return_stdout=False,
    )
    log_result("test_new_auth_unknown_subject", result)
    assert result.returncode != 0
    assert json.loads(result.stdout.decode())["Error"]["CodeString"] == "E_UNAUTHORIZED"
