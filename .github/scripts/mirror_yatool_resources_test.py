import json
import sys
from pathlib import Path

import pytest

from . import mirror_yatool_resources as mirror


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
    patch_out = tmp_path / "localize.patch"
    summary_out = tmp_path / "summary.md"
    comment_out = tmp_path / "comment.md"
    uploaded: list[str] = []
    downloaded: list[str] = []

    def fake_download(url: str, dst: Path, attempts: int) -> str:
        del attempts
        downloaded.append(url)
        dst.write_bytes(b"resource")
        return "md5"

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
            "--local-base-url",
            local_base_url,
            "--patch-out",
            str(patch_out),
            "--summary-out",
            str(summary_out),
            "--comment-out",
            str(comment_out),
        ],
    )

    assert mirror.main() == 0

    assert downloaded == ["https://devtools-registry.s3.yandex.net/6277415837"]
    assert uploaded == ["6277415837"]
    summary = summary_out.read_text(encoding="utf-8")
    assert "Uploaded or refreshed: `1`" in summary
    assert "Already up to date: `1`" in summary


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
    patch_out = tmp_path / "localize.patch"
    summary_out = tmp_path / "summary.md"
    comment_out = tmp_path / "comment.md"

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
            "--local-base-url",
            local_base_url,
            "--patch-out",
            str(patch_out),
            "--summary-out",
            str(summary_out),
            "--comment-out",
            str(comment_out),
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
