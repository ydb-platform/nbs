import asyncio
import argparse
from types import SimpleNamespace

import pytest

from . import nebius_manage_vm as m
from . import helpers as h


def yaml_safe_load_cloud_init(user_data):
    return m.yaml.safe_load(user_data.removeprefix("#cloud-config\n"))


def make_create_args(**overrides):
    args = {
        "parent_id": "parent-id",
        "name": "vm-name",
        "disk_size": 930,
        "disk_type": "network-ssd-nonreplicated",
        "image_id": "image-id",
        "labels": {"test": "librarian"},
        "runner_flavor": "large",
        "platform_id": "cpu-d3",
        "preset": "4vcpu-16gb",
        "subnet_id": "subnet-id",
        "no_public_ip": False,
        "allow_downgrade": False,
        "downgrade_after": 2,
    }
    args.update(overrides)
    return argparse.Namespace(**args)


def test_validate_create_args_accepts_valid_args(monkeypatch):
    monkeypatch.setenv("VM_USER_PASSWD", "encrypted-passwd")
    monkeypatch.setenv("GITHUB_TOKEN", "token")

    m.validate_create_args(
        platform_id="cpu-d3",
        preset="4vcpu-16gb",
        disk_type="network-ssd-nonreplicated",
        disk_size=930,
    )


def test_validate_create_args_rejects_preset_unavailable_for_platform(monkeypatch):
    monkeypatch.setenv("VM_USER_PASSWD", "encrypted-passwd")
    monkeypatch.setenv("GITHUB_TOKEN", "token")

    with pytest.raises(Exception, match="Preset 2vcpu-8gb is not available"):
        m.validate_create_args(
            platform_id="cpu-d3",
            preset="2vcpu-8gb",
            disk_type="network-ssd-nonreplicated",
            disk_size=930,
        )


def test_validate_create_args_requires_vm_user_passwd(monkeypatch):
    monkeypatch.delenv("VM_USER_PASSWD", raising=False)
    monkeypatch.setenv("GITHUB_TOKEN", "token")

    with pytest.raises(
        Exception, match="VM_USER_PASSWD environment variable is not set"
    ):
        m.validate_create_args(
            platform_id="cpu-d3",
            preset="4vcpu-16gb",
            disk_type="network-ssd-nonreplicated",
            disk_size=930,
        )


def test_validate_create_args_requires_github_token(monkeypatch):
    monkeypatch.setenv("VM_USER_PASSWD", "encrypted-passwd")
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)

    with pytest.raises(Exception, match="GITHUB_TOKEN environment variable is not set"):
        m.validate_create_args(
            platform_id="cpu-d3",
            preset="4vcpu-16gb",
            disk_type="network-ssd-nonreplicated",
            disk_size=930,
        )


def test_validate_create_args_rejects_nonreplicated_disk_size_not_multiple_of_93(
    monkeypatch,
):
    monkeypatch.setenv("VM_USER_PASSWD", "encrypted-passwd")
    monkeypatch.setenv("GITHUB_TOKEN", "token")

    with pytest.raises(ValueError, match="multiple of 93GB"):
        m.validate_create_args(
            platform_id="cpu-d3",
            preset="4vcpu-16gb",
            disk_type="network-ssd-nonreplicated",
            disk_size=100,
        )


def test_validate_create_args_allows_other_disk_type_size(monkeypatch):
    monkeypatch.setenv("VM_USER_PASSWD", "encrypted-passwd")
    monkeypatch.setenv("GITHUB_TOKEN", "token")

    m.validate_create_args(
        platform_id="cpu-d3",
        preset="4vcpu-16gb",
        disk_type="network-ssd",
        disk_size=100,
    )


def test_build_disk_request():
    request = m.build_disk_request(
        parent_id="parent-id",
        name="vm-name",
        disk_size=930,
        image_id="image-id",
    )

    assert request.metadata.parent_id == "parent-id"
    assert request.metadata.name == "disk-vm-name"
    assert request.spec.size_gibibytes == 930
    assert request.spec.source_image_id == "image-id"
    assert request.spec.type == m.DiskSpec.DiskType.NETWORK_SSD_NON_REPLICATED


def test_get_runner_token_retries_github_errors(monkeypatch, capsys):
    attempts = []
    sleeps = []

    class FakeRepo:
        def create_self_hosted_runner_registration_token(self):
            attempts.append(1)
            if len(attempts) == 1:
                raise m.GithubException(
                    401,
                    {"message": "Bad credentials", "status": "401"},
                    {},
                )
            return SimpleNamespace(
                token="runner-token",
                expires_at="2026-05-04T16:00:00Z",
            )

    class FakeGithub:
        def __init__(self, auth):
            assert auth == ("token", "github-token")

        def get_repo(self, repo):
            assert repo == "owner/repo"
            return FakeRepo()

    monkeypatch.setattr(m.GithubAuth, "Token", lambda token: ("token", token))
    monkeypatch.setattr(m, "Github", FakeGithub)
    monkeypatch.setattr(m.time, "sleep", sleeps.append)

    assert m.get_runner_token("owner", "repo", "github-token") == "runner-token"
    assert len(attempts) == 2
    assert sleeps == [m.GITHUB_API_RETRY_INTERVAL_SEC]
    assert "::add-mask::runner-token" in capsys.readouterr().out
    assert m.SENSITIVE_DATA_VALUES["runner_token"] == "runner-token"


@pytest.mark.parametrize(
    "exception_type",
    [
        m.requests.exceptions.ConnectionError,
        m.requests.exceptions.Timeout,
    ],
)
def test_get_runner_token_retries_transport_errors(monkeypatch, exception_type):
    attempts = []
    sleeps = []

    class FakeRepo:
        def create_self_hosted_runner_registration_token(self):
            attempts.append(1)
            if len(attempts) == 1:
                raise exception_type("github transport error")
            return SimpleNamespace(
                token="runner-token",
                expires_at="2026-05-04T16:00:00Z",
            )

    class FakeGithub:
        def __init__(self, auth):
            assert auth == ("token", "github-token")

        def get_repo(self, repo):
            assert repo == "owner/repo"
            return FakeRepo()

    monkeypatch.setattr(m.GithubAuth, "Token", lambda token: ("token", token))
    monkeypatch.setattr(m, "Github", FakeGithub)
    monkeypatch.setattr(m.time, "sleep", sleeps.append)

    assert m.get_runner_token("owner", "repo", "github-token") == "runner-token"
    assert len(attempts) == 2
    assert sleeps == [m.GITHUB_API_RETRY_INTERVAL_SEC]


def test_get_runner_token_reports_missing_token_after_retries(monkeypatch):
    attempts = []
    sleeps = []

    class FakeRepo:
        def create_self_hosted_runner_registration_token(self):
            attempts.append(1)
            return SimpleNamespace(token="", expires_at="2026-05-04T16:00:00Z")

    class FakeGithub:
        def __init__(self, auth):
            assert auth == ("token", "github-token")

        def get_repo(self, repo):
            assert repo == "owner/repo"
            return FakeRepo()

    monkeypatch.setattr(m.GithubAuth, "Token", lambda token: ("token", token))
    monkeypatch.setattr(m, "Github", FakeGithub)
    monkeypatch.setattr(m.time, "sleep", sleeps.append)

    with pytest.raises(ValueError) as err:
        m.get_runner_token("owner", "repo", "github-token")

    assert str(err.value) == "Failed to get runner registration token"
    assert len(attempts) == m.GITHUB_API_RETRY_ATTEMPTS
    assert sleeps == [m.GITHUB_API_RETRY_INTERVAL_SEC] * (
        m.GITHUB_API_RETRY_ATTEMPTS - 1
    )


def test_repository_registration_token_extension_uses_pygithub_requester():
    class FakeRequester:
        def requestJsonAndCheck(self, method, url):
            assert method == "POST"
            assert (
                url
                == "https://api.github.com/repos/owner/repo/actions/runners/registration-token"
            )
            return {}, {
                "token": "runner-token",
                "expires_at": "2026-05-04T16:00:00Z",
            }

    class FakeRepo:
        url = "https://api.github.com/repos/owner/repo"
        _requester = FakeRequester()

    registration_token = m._create_repository_self_hosted_runner_registration_token(
        FakeRepo()
    )

    assert registration_token.token == "runner-token"
    assert registration_token.expires_at.isoformat() == "2026-05-04T16:00:00+00:00"


def test_get_latest_github_runner_version_strips_tag_prefix(monkeypatch):
    class FakeRepo:
        def get_latest_release(self):
            payload = make_runner_release_payload("2.332.0")
            return SimpleNamespace(
                tag_name=payload["tag_name"],
                body=payload["body"],
                get_assets=lambda: [
                    SimpleNamespace(name=asset["name"]) for asset in payload["assets"]
                ],
            )

    class FakeGithub:
        def get_repo(self, repo):
            assert repo == "actions/runner"
            return FakeRepo()

    def fake_github_client(token):
        assert token == "github-token"
        return FakeGithub()

    monkeypatch.setattr(h, "github_client", fake_github_client)

    assert h.get_latest_github_runner_version("github-token") == "2.332.0"


def make_runner_release_payload(version="2.332.0"):
    return {
        "tag_name": f"v{version}",
        "body": (
            f"- actions-runner-linux-x64-{version}.tar.gz "
            f"<!-- BEGIN SHA linux-x64 -->{'a' * 64}<!-- END SHA linux-x64 -->\n"
            f"- actions-runner-linux-arm64-{version}.tar.gz "
            f"<!-- BEGIN SHA linux-arm64 -->{'b' * 64}<!-- END SHA linux-arm64 -->\n"
            f"- actions-runner-linux-arm-{version}.tar.gz "
            f"<!-- BEGIN SHA linux-arm -->{'c' * 64}<!-- END SHA linux-arm -->"
        ),
        "assets": [
            {
                "name": f"actions-runner-linux-x64-{version}.tar.gz",
            },
            {
                "name": f"actions-runner-linux-arm64-{version}.tar.gz",
            },
        ],
    }


def test_extract_github_runner_release_returns_arch_sha_from_body():
    release = h.extract_github_runner_release(make_runner_release_payload())

    assert release.version == "2.332.0"
    assert release.sha256_by_arch == {
        "x64": "a" * 64,
        "arm64": "b" * 64,
    }


def test_get_github_runner_release_fetches_tag_and_sha(monkeypatch):
    class FakeRepo:
        def get_release(self, tag):
            assert tag == "v2.331.0"
            payload = make_runner_release_payload("2.331.0")
            return SimpleNamespace(
                tag_name=payload["tag_name"],
                body=payload["body"],
                get_assets=lambda: [
                    SimpleNamespace(name=asset["name"]) for asset in payload["assets"]
                ],
            )

    class FakeGithub:
        def get_repo(self, repo):
            assert repo == "actions/runner"
            return FakeRepo()

    def fake_github_client(token):
        assert token == "github-token"
        return FakeGithub()

    monkeypatch.setattr(h, "github_client", fake_github_client)

    release = h.get_github_runner_release("v2.331.0", "github-token")

    assert release.version == "2.331.0"
    assert release.sha256_by_arch["x64"] == "a" * 64


def test_resolve_github_runner_release_fetches_latest(monkeypatch):
    calls = []

    def fake_get_latest(github_token):
        calls.append(github_token)
        return h.GithubRunnerRelease(
            version="2.332.0",
            sha256_by_arch={"x64": "a" * 64, "arm64": "b" * 64},
        )

    monkeypatch.setattr(h, "get_latest_github_runner_release", fake_get_latest)

    release = h.resolve_github_runner_release("latest", "github-token")

    assert release.version == "2.332.0"
    assert calls == ["github-token"]


def test_resolve_github_runner_version_fetches_latest(monkeypatch):
    calls = []

    def fake_get_latest(github_token):
        calls.append(github_token)
        return h.GithubRunnerRelease(
            version="2.332.0",
            sha256_by_arch={"x64": "a" * 64, "arm64": "b" * 64},
        )

    monkeypatch.setattr(h, "get_latest_github_runner_release", fake_get_latest)

    assert h.resolve_github_runner_version("latest", "github-token") == "2.332.0"
    assert calls == ["github-token"]


def test_resolve_github_runner_version_fetches_literal_release(monkeypatch):
    calls = []

    def fake_get_release(version, github_token):
        calls.append((version, github_token))
        return h.GithubRunnerRelease(
            version="2.331.0",
            sha256_by_arch={"x64": "a" * 64, "arm64": "b" * 64},
        )

    monkeypatch.setattr(h, "get_github_runner_release", fake_get_release)

    assert h.resolve_github_runner_version("v2.331.0", "github-token") == "2.331.0"
    assert calls == [("v2.331.0", "github-token")]


def test_generate_cloud_init_script_renders_repo_template(monkeypatch):
    monkeypatch.setenv("VM_USER_PASSWD", "encrypted-passwd")
    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setenv("GITHUB_SHA", "abc123")

    user_data, ssh_keys = m.generate_cloud_init_script(
        user="github",
        ssh_keys=["ssh-rsa public-key"],
        owner="owner",
        repo="repo",
        token="token with spaces",
        version="2.332.0",
        sha256_by_arch={"x64": "a" * 64, "arm64": "b" * 64},
        override_existing_runner=True,
        label="runner-label",
    )

    cloud_init = yaml_safe_load_cloud_init(user_data)
    script = cloud_init["runcmd"][0]
    assert ssh_keys == ["ssh-rsa public-key"]
    assert cloud_init["ssh_pwauth"] is False
    assert cloud_init["users"][0]["ssh_authorized_keys"] == ["ssh-rsa public-key"]
    assert cloud_init["users"][0]["lock_passwd"] is False
    assert cloud_init["users"][0]["passwd"] == "encrypted-passwd"
    assert cloud_init["users"][0]["name"] == "github"
    assert "sudo" not in cloud_init["users"][0]
    assert cloud_init["users"][1]["name"] == "debug"
    assert cloud_init["users"][1]["ssh_authorized_keys"] == ["ssh-rsa public-key"]
    assert cloud_init["users"][1]["sudo"] == "ALL=(ALL) NOPASSWD:ALL"
    assert cloud_init["users"][1]["lock_passwd"] is False
    assert cloud_init["users"][1]["passwd"] == "encrypted-passwd"
    assert "RUNNER_VERSION=2.332.0" in script
    assert f"RUNNER_SHA256_X64={'a' * 64}" in script
    assert f"RUNNER_SHA256_ARM64={'b' * 64}" in script
    assert "UPDATE_RUNNER=true" in script
    assert "REPO_URL=https://github.com/owner/repo" in script
    assert "RUNNER_USER=github" in script
    assert "RUNNER_REGISTRATION_TOKEN='token with spaces'" in script
    assert "RUNNER_ALLOW_RUNASROOT" not in script
    assert 'runuser -u "$RUNNER_USER" -- ./config.sh' in script
    assert './svc.sh install "$RUNNER_USER"' in script
    assert "kernel.core_pattern=/coredumps/%e.%p.%s" in script
    assert "LimitMEMLOCK=infinity" in script
    assert "kernel.dmesg_restrict=0" not in script
    assert "NOPASSWD:ALL" not in script
    assert "actions-runner-collect-system-logs.sh" in script
    assert "GITHUB_RUNNER_TESTS" not in script
    assert "GITHUB_REPOSITORY_owner_repo" in script
    assert "GITHUB_SHA_abc123" in script


def test_resolve_runner_release_for_update_skips_lookup_when_override_disabled(
    monkeypatch,
):
    def fail_resolve(version, github_token):
        raise AssertionError(f"unexpected release lookup for {version} {github_token}")

    monkeypatch.setattr(m, "resolve_github_runner_release", fail_resolve)

    assert m.resolve_runner_release_for_update("latest", "github-token", "false") == (
        "",
        {},
    )


def test_resolve_runner_release_for_update_resolves_when_override_enabled(monkeypatch):
    calls = []

    def fake_resolve(version, github_token):
        calls.append((version, github_token))
        return h.GithubRunnerRelease(
            version="2.332.0",
            sha256_by_arch={"x64": "a" * 64, "arm64": "b" * 64},
        )

    monkeypatch.setattr(m, "resolve_github_runner_release", fake_resolve)

    assert m.resolve_runner_release_for_update("latest", "github-token", "true") == (
        "2.332.0",
        {"x64": "a" * 64, "arm64": "b" * 64},
    )
    assert calls == [("latest", "github-token")]


def test_wait_runner_by_name_retries_github_lookup_errors(monkeypatch):
    attempts = []

    class FakeRepo:
        def get_self_hosted_runners(self):
            attempts.append(1)
            if len(attempts) == 1:
                raise RuntimeError("github api is temporarily unavailable")
            return [SimpleNamespace(name="vm-id", id="runner-id")]

    class FakeGithub:
        def get_repo(self, repo_name):
            assert repo_name == "owner/repo"
            return FakeRepo()

    sleeps = []

    monkeypatch.setattr(m.time, "sleep", sleeps.append)

    assert m.wait_runner_by_name(FakeGithub(), "owner", "repo", "vm-id") == "runner-id"
    assert len(attempts) == 2
    assert sleeps == [m.RUNNER_REGISTRATION_RETRY_INTERVAL_SEC]


def test_wait_runner_by_name_retries_missing_runner(monkeypatch):
    runners_by_attempt = [
        [SimpleNamespace(name="another-vm", id="another-runner-id")],
        [SimpleNamespace(name="vm-id", id="runner-id")],
    ]
    sleeps = []

    class FakeRepo:
        def get_self_hosted_runners(self):
            return runners_by_attempt.pop(0)

    class FakeGithub:
        def get_repo(self, repo_name):
            assert repo_name == "owner/repo"
            return FakeRepo()

    monkeypatch.setattr(m.time, "sleep", sleeps.append)

    assert m.wait_runner_by_name(FakeGithub(), "owner", "repo", "vm-id") == "runner-id"
    assert runners_by_attempt == []
    assert sleeps == [m.RUNNER_REGISTRATION_RETRY_INTERVAL_SEC]


def test_find_runner_by_name_does_not_retry_missing_runner(monkeypatch):
    attempts = []
    sleeps = []

    class FakeRepo:
        def get_self_hosted_runners(self):
            attempts.append(1)
            return [SimpleNamespace(name="another-vm", id="another-runner-id")]

    class FakeGithub:
        def get_repo(self, repo_name):
            assert repo_name == "owner/repo"
            return FakeRepo()

    monkeypatch.setattr(m.time, "sleep", sleeps.append)

    assert m.find_runner_by_name(FakeGithub(), "owner", "repo", "vm-id") is None
    assert len(attempts) == 1
    assert sleeps == []


def test_remove_runner_from_github_retries_github_lookup_errors(monkeypatch):
    list_runner_attempts = []
    get_runner_attempts = []
    sleeps = []

    class FakeRepo:
        def get_self_hosted_runners(self):
            list_runner_attempts.append(1)
            return [SimpleNamespace(name="vm-id", id="runner-id")]

        def get_self_hosted_runner(self, runner_id):
            assert runner_id == "runner-id"
            get_runner_attempts.append(1)
            if len(get_runner_attempts) == 1:
                raise m.GithubException(
                    401,
                    {"message": "Bad credentials", "status": "401"},
                    {},
                )
            return SimpleNamespace(
                name="vm-id",
                id="runner-id",
                status="offline",
                busy=False,
            )

    class FakeGithub:
        def get_repo(self, repo_name):
            assert repo_name == "owner/repo"
            return FakeRepo()

    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setattr(m.time, "sleep", sleeps.append)

    assert (
        m.remove_runner_from_github(FakeGithub(), "owner", "repo", "vm-id", False)
        == "would_remove"
    )
    assert len(list_runner_attempts) == 1
    assert len(get_runner_attempts) == 2
    assert sleeps == [m.GITHUB_API_RETRY_INTERVAL_SEC]


def test_remove_runner_from_github_retries_github_remove_errors(monkeypatch):
    list_runner_attempts = []
    remove_runner_attempts = []
    sleeps = []

    class FakeRepo:
        def get_self_hosted_runners(self):
            list_runner_attempts.append(1)
            return [SimpleNamespace(name="vm-id", id="runner-id")]

        def get_self_hosted_runner(self, runner_id):
            assert runner_id == "runner-id"
            return SimpleNamespace(
                name="vm-id",
                id="runner-id",
                status="offline",
                busy=False,
            )

        def remove_self_hosted_runner(self, runner_id):
            assert runner_id == "runner-id"
            remove_runner_attempts.append(1)
            if len(remove_runner_attempts) == 1:
                raise m.GithubException(
                    401,
                    {"message": "Bad credentials", "status": "401"},
                    {},
                )
            return True

    class FakeGithub:
        def get_repo(self, repo_name):
            assert repo_name == "owner/repo"
            return FakeRepo()

    monkeypatch.setenv("GITHUB_REPOSITORY", "owner/repo")
    monkeypatch.setattr(m.time, "sleep", sleeps.append)

    assert (
        m.remove_runner_from_github(FakeGithub(), "owner", "repo", "vm-id", True)
        == "removed"
    )
    assert len(list_runner_attempts) == 1
    assert len(remove_runner_attempts) == 2
    assert sleeps == [m.GITHUB_API_RETRY_INTERVAL_SEC]


def test_retry_retries_async_function_and_passes_attempt(monkeypatch):
    attempts = []
    sleeps = []

    async def fake_sleep(interval):
        sleeps.append(interval)

    async def flaky(attempt=0):
        attempts.append(attempt)
        if attempt < 2:
            raise m.RequestError(SimpleNamespace())
        return "ok"

    monkeypatch.setattr(h.asyncio, "sleep", fake_sleep)

    retried = h.retry(
        attempts=3,
        interval_sec=7,
        retry_exceptions=(m.RequestError,),
        attempt_arg="attempt",
    )(flaky)

    assert asyncio.run(retried()) == "ok"
    assert attempts == [0, 1, 2]
    assert sleeps == [7, 7]


def test_retry_calls_final_exception_hook_for_async_function(monkeypatch):
    sleeps = []
    final_exceptions = []
    attempts = []

    async def fake_sleep(interval):
        sleeps.append(interval)

    async def always_fails(attempt=0):
        attempts.append(attempt)
        raise m.RequestError(SimpleNamespace())

    monkeypatch.setattr(h.asyncio, "sleep", fake_sleep)

    retried = h.retry(
        attempts=2,
        interval_sec=7,
        retry_exceptions=(m.RequestError,),
        attempt_arg="attempt",
        on_final_exception=final_exceptions.append,
    )(always_fails)

    with pytest.raises(m.RequestError):
        asyncio.run(retried())

    assert len(final_exceptions) == 1
    assert attempts == [0, 1]
    assert sleeps == [7]


def test_create_disk_removes_created_disk_when_wait_fails(monkeypatch):
    class FakeDiskOperation:
        resource_id = "disk-id"

        async def wait(self):
            raise m.RequestError(SimpleNamespace())

    class FakeDiskService:
        async def create(self, request):
            self.request = request
            return FakeDiskOperation()

    removed = []

    async def fake_remove_disk_by_id(_sdk, args, disk_id):
        removed.append((_sdk, args, disk_id))

    fake_service = FakeDiskService()
    sdk = object()
    args = make_create_args(apply=True)
    created_clients = []

    def fake_disk_service_client(sdk_arg):
        created_clients.append(sdk_arg)
        return fake_service

    monkeypatch.setattr(m, "DiskServiceClient", fake_disk_service_client)
    monkeypatch.setattr(m, "remove_disk_by_id", fake_remove_disk_by_id)

    with pytest.raises(m.RequestError):
        asyncio.run(m.create_disk(sdk, args))

    assert fake_service.request.metadata.name == "disk-vm-name"
    assert created_clients == [sdk]
    assert removed == [(sdk, args, "disk-id")]


def test_build_vm_labels_adds_runner_metadata_to_copy_of_existing_labels():
    original_labels = {"test": "librarian"}

    labels = m.build_vm_labels(original_labels, "runner-label", "large")

    assert labels == {
        "test": "librarian",
        "runner-label": "runner-label",
        "runner-flavor": "large",
    }
    assert original_labels == {"test": "librarian"}


def test_build_instance_request_with_public_ip():
    labels = {"test": "librarian", "runner-label": "runner-label"}

    request = m.build_instance_request(
        parent_id="parent-id",
        name="vm-name",
        platform_id="cpu-d3",
        preset="4vcpu-16gb",
        subnet_id="subnet-id",
        no_public_ip=False,
        disk_id="disk-id",
        user_data="cloud-init",
        labels=labels,
    )

    assert request.metadata.parent_id == "parent-id"
    assert request.metadata.name == "vm-name"
    assert request.metadata.labels == labels
    assert request.spec.cloud_init_user_data == "cloud-init"
    assert request.spec.resources.platform == "cpu-d3"
    assert request.spec.resources.preset == "4vcpu-16gb"
    assert request.spec.boot_disk.existing_disk.id == "disk-id"
    assert request.spec.boot_disk.device_id == "boot"
    assert (
        request.spec.boot_disk.attach_mode == m.AttachedDiskSpec.AttachMode.READ_WRITE
    )

    network_interface = request.spec.network_interfaces[0]
    assert network_interface.name == "eth0"
    assert network_interface.subnet_id == "subnet-id"
    assert network_interface.public_ip_address is not None


def test_build_instance_request_without_public_ip():
    request = m.build_instance_request(
        parent_id="parent-id",
        name="vm-name",
        platform_id="cpu-d3",
        preset="4vcpu-16gb",
        subnet_id="subnet-id",
        no_public_ip=True,
        disk_id="disk-id",
        user_data="cloud-init",
        labels={},
    )

    assert request.spec.network_interfaces[0].public_ip_address is None


@pytest.mark.parametrize(
    "attempt,expected_preset",
    [
        pytest.param(0, "16vcpu-64gb", id="first-attempt-does-not-downgrade"),
        pytest.param(1, "16vcpu-64gb", id="before-boundary-does-not-downgrade"),
        pytest.param(2, "8vcpu-32gb", id="boundary-downgrades-one-step"),
    ],
)
def test_maybe_downgrade_preset(attempt, expected_preset):
    preset = m.maybe_downgrade_preset(
        platform_id="cpu-d3",
        preset="16vcpu-64gb",
        allow_downgrade=True,
        downgrade_after=2,
        attempt=attempt,
    )

    assert preset == expected_preset


def test_maybe_downgrade_preset_keeps_minimum_preset():
    preset = m.maybe_downgrade_preset(
        platform_id="cpu-d3",
        preset="4vcpu-16gb",
        allow_downgrade=True,
        downgrade_after=2,
        attempt=2,
    )

    assert preset == "4vcpu-16gb"


def test_maybe_downgrade_preset_respects_disabled_flag():
    preset = m.maybe_downgrade_preset(
        platform_id="cpu-d3",
        preset="16vcpu-64gb",
        allow_downgrade=False,
        downgrade_after=2,
        attempt=2,
    )

    assert preset == "16vcpu-64gb"


def test_extract_instance_ips_with_public_ip():
    instance = SimpleNamespace(
        status=SimpleNamespace(
            network_interfaces=[
                SimpleNamespace(
                    ip_address=SimpleNamespace(address="10.0.0.5/32"),
                    public_ip_address=SimpleNamespace(address="198.51.100.10/32"),
                )
            ]
        )
    )

    assert m.extract_instance_ips(instance, no_public_ip=False) == (
        "10.0.0.5",
        "198.51.100.10",
    )


def test_extract_instance_ips_without_public_ip():
    instance = SimpleNamespace(
        status=SimpleNamespace(
            network_interfaces=[
                SimpleNamespace(
                    ip_address=SimpleNamespace(address="10.0.0.5/32"),
                    public_ip_address=None,
                )
            ]
        )
    )

    assert m.extract_instance_ips(instance, no_public_ip=True) == ("10.0.0.5", None)


def test_labels_match_reports_mismatches():
    matched, mismatches = m.labels_match(
        {"run": "1-1", "repo": "nbs"},
        {"run": "1-1", "repo": "nbs", "owner": "ydb-platform"},
    )

    assert matched is False
    assert mismatches == ["owner: expected 'ydb-platform', actual None"]


def test_search_vm_cleanup_candidates_by_labels_reports_matching_vm_and_disk(
    monkeypatch,
):
    matching_instance = SimpleNamespace(
        metadata=SimpleNamespace(
            id="computeinstance-match",
            name="runner-match",
            labels={"run": "1-1", "repo": "nbs", "owner": "ydb-platform"},
        )
    )
    non_matching_instance = SimpleNamespace(
        metadata=SimpleNamespace(
            id="computeinstance-other",
            name="runner-other",
            labels={"run": "2-1", "repo": "nbs", "owner": "ydb-platform"},
        )
    )
    listed_requests = []
    disk_requests = []

    class FakeInstanceService:
        async def list(self, request):
            listed_requests.append(request)
            return SimpleNamespace(
                items=[non_matching_instance, matching_instance],
                next_page_token="",
            )

    class FakeDiskService:
        async def get_by_name(self, request):
            disk_requests.append(request)
            return SimpleNamespace(
                metadata=SimpleNamespace(
                    id="disk-id",
                    name="disk-runner-match",
                )
            )

    def fake_instance_service_client(sdk):
        assert sdk is fake_sdk
        return FakeInstanceService()

    def fake_disk_service_client(sdk):
        assert sdk is fake_sdk
        return FakeDiskService()

    fake_sdk = object()

    monkeypatch.setattr(m, "InstanceServiceClient", fake_instance_service_client)
    monkeypatch.setattr(m, "DiskServiceClient", fake_disk_service_client)

    args = argparse.Namespace(
        parent_id="parent-id",
        labels={"run": "1-1", "repo": "nbs", "owner": "ydb-platform"},
    )

    candidates = asyncio.run(m.search_vm_cleanup_candidates_by_labels(fake_sdk, args))

    assert candidates == [matching_instance]
    assert listed_requests[0].parent_id == "parent-id"
    assert disk_requests[0].name == "disk-runner-match"


def test_remove_vm_with_empty_id_only_searches_by_labels(monkeypatch):
    searches = []

    async def fake_search(sdk, args):
        searches.append((sdk, args))
        return []

    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    monkeypatch.setattr(m, "search_vm_cleanup_candidates_by_labels", fake_search)

    sdk = object()
    args = argparse.Namespace(id="", labels={"run": "1-1"})

    asyncio.run(m.remove_vm(sdk, args))

    assert searches == [(sdk, args)]
