#!/usr/bin/env python3
from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import shutil
import sys
import tempfile
import time
import urllib.request
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

DEFAULT_LOCAL_BASE_URL = "https://storage.eu-north2.nebius.cloud/nbs-yatool-resources"
ALLOWED_SOURCE_HOSTS = frozenset({"devtools-registry.s3.yandex.net"})


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as fp:
        return json.load(fp)


def dump_json(data: dict[str, Any]) -> str:
    return json.dumps(data, indent=4, ensure_ascii=False) + "\n"


def validate_source_url(resource_id: str, url: str) -> str:
    if not resource_id.isdigit():
        raise ValueError(f"resource id must be numeric: {resource_id}")

    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise ValueError(f"resource {resource_id} must use https URL")
    if parsed.hostname not in ALLOWED_SOURCE_HOSTS:
        raise ValueError(
            f"resource {resource_id} host {parsed.hostname!r} is not allowed"
        )
    if parsed.username or parsed.password or parsed.port:
        raise ValueError(
            f"resource {resource_id} URL must not contain authority extras"
        )
    if parsed.query or parsed.fragment:
        raise ValueError(
            f"resource {resource_id} URL must not contain query or fragment"
        )
    if parsed.path != f"/{resource_id}":
        raise ValueError(
            f"resource {resource_id} URL path must be exactly /{resource_id}"
        )
    return url


def validate_local_base_url(local_base_url: str) -> str:
    base = local_base_url.rstrip("/")
    parsed = urlparse(base)
    if parsed.scheme != "https" or not parsed.hostname:
        raise ValueError(
            f"local base URL must be a full https URL, got {local_base_url!r}"
        )
    if parsed.username or parsed.password or parsed.port:
        raise ValueError("local base URL must not contain authority extras")
    if parsed.query or parsed.fragment:
        raise ValueError("local base URL must not contain query or fragment")
    return base


def is_localized_resource_url(resource_id: str, url: str, local_base_url: str) -> bool:
    if not resource_id.isdigit():
        return False

    base = validate_local_base_url(local_base_url)
    parsed_base = urlparse(base)
    parsed_url = urlparse(url)
    if parsed_url.scheme != parsed_base.scheme:
        return False
    if parsed_url.hostname != parsed_base.hostname:
        return False
    if parsed_url.username or parsed_url.password or parsed_url.port:
        return False
    if parsed_url.query or parsed_url.fragment:
        return False

    base_path = parsed_base.path.rstrip("/")
    expected_path = f"{base_path}/{resource_id}" if base_path else f"/{resource_id}"
    return parsed_url.path == expected_path


def download(url: str, dst: Path, attempts: int) -> str:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        md5 = hashlib.md5()
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "nbs-resource-mirror"}
            )
            with urllib.request.urlopen(req, timeout=120) as response, dst.open(
                "wb"
            ) as fp:
                shutil.copyfileobj(HashingReader(response, md5), fp)
            return md5.hexdigest()
        except Exception as error:  # noqa: BLE001
            last_error = error
            if dst.exists():
                dst.unlink()
            if attempt < attempts:
                time.sleep(min(30, 2 * attempt))
    raise RuntimeError(f"failed to download {url}: {last_error}")


class HashingReader:
    def __init__(self, raw: Any, digest: Any) -> None:
        self.raw = raw
        self.digest = digest

    def read(self, size: int = -1) -> bytes:
        data = self.raw.read(size)
        if data:
            self.digest.update(data)
        return data


def make_s3_client(endpoint_url: str | None) -> Any:
    return boto3.client("s3", endpoint_url=endpoint_url or None)


def is_not_found(error: ClientError) -> bool:
    code = error.response.get("Error", {}).get("Code", "")
    return code in {"404", "NoSuchKey", "NotFound"}


def get_existing_md5(s3: Any, bucket: str, key: str) -> str:
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if is_not_found(error):
            return ""
        raise

    metadata = head.get("Metadata") or {}
    md5 = metadata.get("md5", "").strip()
    if md5:
        return md5

    try:
        sidecar = s3.get_object(Bucket=bucket, Key=f"{key}.md5")
        sidecar_md5 = sidecar["Body"].read().decode("utf-8").strip()
        if sidecar_md5:
            return sidecar_md5
    except ClientError as error:
        if not is_not_found(error):
            raise

    etag = str(head.get("ETag", "")).strip('"')
    return etag if "-" not in etag else ""


def upload_resource(s3: Any, bucket: str, key: str, path: Path, md5: str) -> None:
    s3.upload_file(
        str(path),
        bucket,
        key,
        ExtraArgs={
            "ACL": "public-read",
            "Metadata": {"md5": md5},
            "ContentType": "application/octet-stream",
        },
    )
    s3.put_object(
        Bucket=bucket,
        Key=f"{key}.md5",
        Body=(md5 + "\n").encode("utf-8"),
        ACL="public-read",
        ContentType="text/plain",
    )


def localize_mapping(mapping_path: Path, local_base_url: str) -> str:
    data = load_json(mapping_path)
    resources = data.get("resources")
    if not isinstance(resources, dict):
        raise ValueError(f"{mapping_path} does not contain a resources object")

    base = validate_local_base_url(local_base_url)
    for resource_id in list(resources):
        resources[resource_id] = f"{base}/{resource_id}"

    return dump_json(data)


def write_patch(mapping_path: Path, localized_text: str, patch_out: Path) -> None:
    original = mapping_path.read_text(encoding="utf-8").splitlines(keepends=True)
    localized = localized_text.splitlines(keepends=True)
    diff = difflib.unified_diff(
        original,
        localized,
        fromfile=f"a/{mapping_path.as_posix()}",
        tofile=f"b/{mapping_path.as_posix()}",
    )
    patch_out.write_text("".join(diff), encoding="utf-8")


def resource_delta(
    mapping_path: Path, base_mapping_path: Path | None
) -> tuple[list[str], list[str]]:
    if base_mapping_path is None or not base_mapping_path.exists():
        return [], []
    current = set(load_json(mapping_path).get("resources", {}))
    base = set(load_json(base_mapping_path).get("resources", {}))
    return sorted(current - base), sorted(base - current)


def write_summary(
    summary_out: Path,
    *,
    total: int,
    uploaded: list[str],
    skipped: list[str],
    added: list[str],
    removed: list[str],
    patch_out: Path,
    local_base_url: str,
) -> None:
    lines = [
        "# Yatool resources mirror",
        "",
        f"Local base URL: `{local_base_url.rstrip('/')}`",
        f"Resources in mapping: `{total}`",
        f"Uploaded or refreshed: `{len(uploaded)}`",
        f"Already up to date: `{len(skipped)}`",
        f"Added relative to base mapping: `{len(added)}`",
        f"Removed relative to base mapping: `{len(removed)}`",
        f"Patch file: `{patch_out}`",
        "",
    ]
    if uploaded:
        lines += ["Uploaded resource ids:", ", ".join(uploaded[:80])]
        if len(uploaded) > 80:
            lines.append(f"... and {len(uploaded) - 80} more")
        lines.append("")
    if added:
        lines += ["Added resource ids:", ", ".join(added[:80]), ""]
    if removed:
        lines += ["Removed resource ids:", ", ".join(removed[:80]), ""]
    summary_out.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mapping", type=Path, default=Path("build/mapping.conf.json"))
    parser.add_argument("--base-mapping", type=Path)
    parser.add_argument("--bucket", default="nbs-yatool-resources")
    parser.add_argument("--endpoint-url", default="")
    parser.add_argument("--local-base-url", default=DEFAULT_LOCAL_BASE_URL)
    parser.add_argument("--patch-out", type=Path, required=True)
    parser.add_argument("--summary-out", type=Path, required=True)
    parser.add_argument("--download-attempts", type=int, default=3)
    parser.add_argument("--skip-upload", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    local_base_url = validate_local_base_url(args.local_base_url)
    mapping = load_json(args.mapping)
    resources = mapping.get("resources")
    if not isinstance(resources, dict):
        raise ValueError(f"{args.mapping} does not contain a resources object")

    args.patch_out.parent.mkdir(parents=True, exist_ok=True)
    args.summary_out.parent.mkdir(parents=True, exist_ok=True)

    localized_text = localize_mapping(args.mapping, local_base_url)
    write_patch(args.mapping, localized_text, args.patch_out)

    uploaded: list[str] = []
    skipped: list[str] = []
    if not args.skip_upload:
        s3 = make_s3_client(args.endpoint_url)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            for resource_id, url in sorted(
                resources.items(), key=lambda item: int(item[0])
            ):
                if is_localized_resource_url(resource_id, str(url), local_base_url):
                    skipped.append(resource_id)
                    continue

                dst = tmp_dir / resource_id
                source_url = validate_source_url(resource_id, str(url))
                md5 = download(source_url, dst, args.download_attempts)
                existing_md5 = get_existing_md5(s3, args.bucket, resource_id)
                if existing_md5 == md5:
                    skipped.append(resource_id)
                    continue
                upload_resource(s3, args.bucket, resource_id, dst, md5)
                uploaded.append(resource_id)

    added, removed = resource_delta(args.mapping, args.base_mapping)
    write_summary(
        args.summary_out,
        total=len(resources),
        uploaded=uploaded,
        skipped=skipped,
        added=added,
        removed=removed,
        patch_out=args.patch_out,
        local_base_url=local_base_url,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:  # noqa: BLE001
        print(f"error: {error}", file=sys.stderr)
        raise
