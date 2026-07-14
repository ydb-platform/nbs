import json
import sys
from pathlib import Path

import pytest

from . import mirror_yatool_resources as mirror


def write_bootstrap_script(
    path: Path,
    resources: tuple[tuple[str, str, str], ...] = (
        ("linux", "8580483288", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
    ),
) -> Path:
    platforms = []
    for platform, resource_id, md5 in resources:
        platforms.append(f"""        "{platform}": {{
            "md5": "{md5}",
            "urls": [f"{{REGISTRY_ENDPOINT}}/{resource_id}"]
        }}""")
    path.write_text(
        """import os

REGISTRY_ENDPOINT = os.environ.get(
    "YA_REGISTRY_ENDPOINT", "https://devtools-registry.s3.yandex.net"
)
# Start of mapping
PLATFORM_MAP = {
    "data": {
"""
        + ",\n".join(platforms)
        + """
    }
}
# End of mapping
""",
        encoding="utf-8",
    )
    return path


def test_load_bootstrap_config_extracts_all_platform_resources(
    tmp_path: Path,
) -> None:
    bootstrap_script = write_bootstrap_script(
        tmp_path / "ya",
        (
            ("linux", "8580483288", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
            ("darwin", "8580479378", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"),
        ),
    )

    config = mirror.load_bootstrap_config(bootstrap_script)

    assert config.registry_endpoint == "https://devtools-registry.s3.yandex.net"
    assert config.resources == {
        "8580483288": mirror.BootstrapResource(
            source_url="https://devtools-registry.s3.yandex.net/8580483288",
            md5="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ),
        "8580479378": mirror.BootstrapResource(
            source_url="https://devtools-registry.s3.yandex.net/8580479378",
            md5="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        ),
    }


def test_localize_bootstrap_script_replaces_only_default_endpoint(
    tmp_path: Path,
) -> None:
    bootstrap_script = write_bootstrap_script(tmp_path / "ya")
    config = mirror.load_bootstrap_config(bootstrap_script)

    localized = mirror.localize_bootstrap_script(
        config, "https://mirror.example/resources/"
    )

    assert '"YA_REGISTRY_ENDPOINT", "https://mirror.example/resources"' in localized
    assert 'f"{REGISTRY_ENDPOINT}/8580483288"' in localized


def test_extract_platform_map_rejects_executable_python() -> None:
    source = """# Start of mapping
PLATFORM_MAP = {
    "data": {
        "linux": {
            "md5": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "urls": [f"{REGISTRY_ENDPOINT}/8580483288"]
        }
    },
    "unexpected": dangerous_function()
}
# End of mapping
"""

    with pytest.raises(ValueError, match="not in the expected format"):
        mirror.extract_platform_map(source, "https://devtools-registry.s3.yandex.net")


def test_validate_source_url_accepts_devtools_registry_resource() -> None:
    url = "https://devtools-registry.s3.yandex.net/6277415836"

    assert mirror.validate_source_url("6277415836", url) == url


@pytest.mark.parametrize(
    "resource_id,url",
    [
        ("6277415836", "file:///home/github/.aws/credentials"),
        ("6277415836", "http://devtools-registry.s3.yandex.net/6277415836"),
        ("6277415836", "https://169.254.169.254/latest/meta-data/"),
        ("6277415836", "https://devtools-registry.s3.yandex.net/other"),
        ("6277415836", "https://devtools-registry.s3.yandex.net/6277415836?x=1"),
        ("6277415836", "https://user@devtools-registry.s3.yandex.net/6277415836"),
        ("not-numeric", "https://devtools-registry.s3.yandex.net/not-numeric"),
    ],
)
def test_validate_source_url_rejects_untrusted_urls(resource_id: str, url: str) -> None:
    with pytest.raises(ValueError):
        mirror.validate_source_url(resource_id, url)


def test_validate_local_base_url_accepts_full_bucket_url() -> None:
    url = "https://nbs-yatool-resources.storage.eu-north2.nebius.cloud/"

    assert (
        mirror.validate_local_base_url(url)
        == "https://nbs-yatool-resources.storage.eu-north2.nebius.cloud"
    )


@pytest.mark.parametrize(
    "url",
    [
        "storage.eu-north2.nebius.cloud",
        "http://nbs-yatool-resources.storage.eu-north2.nebius.cloud",
        "https://nbs-yatool-resources.storage.eu-north2.nebius.cloud/?x=1",
        "https://user@nbs-yatool-resources.storage.eu-north2.nebius.cloud",
    ],
)
def test_validate_local_base_url_rejects_invalid_urls(url: str) -> None:
    with pytest.raises(ValueError):
        mirror.validate_local_base_url(url)


@pytest.mark.parametrize(
    "local_base_url,url",
    [
        (
            "https://nbs-yatool-resources.storage.eu-north2.nebius.cloud",
            "https://nbs-yatool-resources.storage.eu-north2.nebius.cloud/6277415836",
        ),
        (
            "https://storage.eu-north2.nebius.cloud/nbs-yatool-resources",
            "https://storage.eu-north2.nebius.cloud/nbs-yatool-resources/6277415836",
        ),
    ],
)
def test_is_localized_resource_url_accepts_configured_mirror_url(
    local_base_url: str, url: str
) -> None:
    assert mirror.is_localized_resource_url("6277415836", url, local_base_url)


@pytest.mark.parametrize(
    "url",
    [
        "https://nbs-yatool-resources.storage.eu-north2.nebius.cloud/other",
        "https://other.storage.eu-north2.nebius.cloud/6277415836",
        "https://nbs-yatool-resources.storage.eu-north2.nebius.cloud/6277415836?x=1",
    ],
)
def test_is_localized_resource_url_rejects_non_matching_urls(url: str) -> None:
    assert not mirror.is_localized_resource_url(
        "6277415836",
        url,
        "https://nbs-yatool-resources.storage.eu-north2.nebius.cloud",
    )


def test_main_skips_already_localized_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    local_base_url = "https://nbs-yatool-resources.storage.eu-north2.nebius.cloud"
    mapping = tmp_path / "mapping.conf.json"
    mapping.write_text(
        json.dumps(
            {
                "resources": {
                    "6277415836": f"{local_base_url}/6277415836",
                    "6277415837": "https://devtools-registry.s3.yandex.net/6277415837",
                }
            }
        ),
        encoding="utf-8",
    )
    bootstrap_script = write_bootstrap_script(tmp_path / "ya")
    patch_out = tmp_path / "localize.patch"
    summary_out = tmp_path / "summary.md"
    uploaded: list[str] = []
    downloaded: list[str] = []

    def fake_download(url: str, dst: Path, attempts: int) -> str:
        del attempts
        downloaded.append(url)
        dst.write_bytes(b"resource")
        if url.endswith("/8580483288"):
            return "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        return "mapping-md5"

    def fake_upload_resource(
        s3: object, bucket: str, key: str, path: Path, md5: str
    ) -> None:
        del s3, bucket, path, md5
        uploaded.append(key)

    def fake_make_s3_client(endpoint_url: str | None) -> object:
        del endpoint_url
        return object()

    def fake_get_existing_md5(s3: object, bucket: str, key: str) -> str:
        del s3, bucket, key
        return ""

    monkeypatch.setattr(mirror, "make_s3_client", fake_make_s3_client)
    monkeypatch.setattr(mirror, "get_existing_md5", fake_get_existing_md5)
    monkeypatch.setattr(mirror, "upload_resource", fake_upload_resource)
    monkeypatch.setattr(mirror, "download", fake_download)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mirror_yatool_resources.py",
            "--mapping",
            str(mapping),
            "--bootstrap-script",
            str(bootstrap_script),
            "--local-base-url",
            local_base_url,
            "--patch-out",
            str(patch_out),
            "--summary-out",
            str(summary_out),
        ],
    )

    assert mirror.main() == 0

    assert downloaded == [
        "https://devtools-registry.s3.yandex.net/6277415837",
        "https://devtools-registry.s3.yandex.net/8580483288",
    ]
    assert uploaded == ["6277415837", "8580483288"]
    summary = summary_out.read_text(encoding="utf-8")
    assert "Bootstrap resources in ya: `1`" in summary
    assert "Uploaded or refreshed: `2`" in summary
    assert "Already up to date: `1`" in summary
    patch = patch_out.read_text(encoding="utf-8")
    assert f'+    "YA_REGISTRY_ENDPOINT", "{local_base_url}"' in patch


def test_main_rejects_bootstrap_resource_with_wrong_md5(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mapping = tmp_path / "mapping.conf.json"
    mapping.write_text('{"resources": {}}', encoding="utf-8")
    bootstrap_script = write_bootstrap_script(tmp_path / "ya")

    def fake_download(url: str, dst: Path, attempts: int) -> str:
        del url, attempts
        dst.write_bytes(b"corrupted")
        return "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

    def fail_upload(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("a bootstrap resource with the wrong md5 must not upload")

    def fake_make_s3_client(endpoint_url: str | None) -> object:
        del endpoint_url
        return object()

    def fake_get_existing_md5(s3: object, bucket: str, key: str) -> str:
        del s3, bucket, key
        return ""

    monkeypatch.setattr(mirror, "make_s3_client", fake_make_s3_client)
    monkeypatch.setattr(mirror, "get_existing_md5", fake_get_existing_md5)
    monkeypatch.setattr(mirror, "download", fake_download)
    monkeypatch.setattr(mirror, "upload_resource", fail_upload)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mirror_yatool_resources.py",
            "--mapping",
            str(mapping),
            "--bootstrap-script",
            str(bootstrap_script),
            "--patch-out",
            str(tmp_path / "localize.patch"),
            "--summary-out",
            str(tmp_path / "summary.md"),
        ],
    )

    with pytest.raises(ValueError, match="bootstrap resource 8580483288 md5 mismatch"):
        mirror.main()


def test_main_skip_upload_does_not_download_resources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    local_base_url = "https://nbs-yatool-resources.storage.eu-north2.nebius.cloud"
    mapping = tmp_path / "mapping.conf.json"
    mapping.write_text(
        json.dumps(
            {
                "resources": {
                    "6277415836": "https://devtools-registry.s3.yandex.net/6277415836",
                    "6277415837": f"{local_base_url}/6277415837",
                }
            }
        ),
        encoding="utf-8",
    )
    bootstrap_script = write_bootstrap_script(tmp_path / "ya")
    patch_out = tmp_path / "localize.patch"
    summary_out = tmp_path / "summary.md"

    def fail_download(url: str, dst: Path, attempts: int) -> str:
        del url, dst, attempts
        raise AssertionError("skip-upload must not download resources")

    monkeypatch.setattr(mirror, "download", fail_download)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "mirror_yatool_resources.py",
            "--mapping",
            str(mapping),
            "--bootstrap-script",
            str(bootstrap_script),
            "--local-base-url",
            local_base_url,
            "--patch-out",
            str(patch_out),
            "--summary-out",
            str(summary_out),
            "--skip-upload",
        ],
    )

    assert mirror.main() == 0

    summary = summary_out.read_text(encoding="utf-8")
    assert "Uploaded or refreshed: `0`" in summary
    assert "Already up to date: `0`" in summary
    assert f'"6277415836": "{local_base_url}/6277415836"' in patch_out.read_text(
        encoding="utf-8"
    )
