#!/usr/bin/env python3
from __future__ import annotations

import argparse
import difflib
import hashlib
import json
import re
import shutil
import sys
import tempfile
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError

DEFAULT_LOCAL_BASE_URL = "https://storage.eu-north2.nebius.cloud/nbs-yatool-resources"
ALLOWED_SOURCE_HOSTS = frozenset({"devtools-registry.s3.yandex.net"})
MD5_RE = re.compile(r"[0-9a-f]{32}")
PLATFORM_MAP_START = "# Start of mapping"
PLATFORM_MAP_END = "# End of mapping"
PLATFORM_MAP_ASSIGNMENT_RE = re.compile(
    r"\s*PLATFORM_MAP\s*=\s*(?P<value>\{.*\})\s*", re.DOTALL
)
REGISTRY_ENDPOINT_RE = re.compile(
    r"REGISTRY_ENDPOINT\s*=\s*os\.environ\.get\(\s*"
    r"[\"']YA_REGISTRY_ENDPOINT[\"']\s*,\s*"
    r"(?P<quote>[\"'])(?P<endpoint>https://[^\"']+)(?P=quote)\s*\)"
)
REGISTRY_URL_RE = re.compile(
    r"f(?P<quote>[\"'])\{REGISTRY_ENDPOINT\}(?P<path>/[0-9]+)(?P=quote)"
)


@dataclass(frozen=True)
class BootstrapResource:
    source_url: str
    md5: str


@dataclass(frozen=True)
class BootstrapConfig:
    source: str
    registry_endpoint: str
    registry_endpoint_start: int
    registry_endpoint_end: int
    resources: dict[str, BootstrapResource]


def load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as fp:
        return json.load(fp)


def dump_json(data: dict[str, Any]) -> str:
    return json.dumps(data, indent=4, ensure_ascii=False) + "\n"


def extract_platform_map(source: str, registry_endpoint: str) -> dict[str, Any]:
    # PLATFORM_MAP is generated between stable marker comments. Restrict parsing
    # to that block so the rest of the shell/Python polyglot script is irrelevant.
    if source.count(PLATFORM_MAP_START) != 1 or source.count(PLATFORM_MAP_END) != 1:
        raise ValueError("ya must contain exactly one generated PLATFORM_MAP block")
    block_start = source.index(PLATFORM_MAP_START) + len(PLATFORM_MAP_START)
    block_end = source.find(PLATFORM_MAP_END, block_start)
    if block_end < 0:
        raise ValueError("PLATFORM_MAP end marker must follow its start marker")
    block = source[block_start:block_end]
    assignment = PLATFORM_MAP_ASSIGNMENT_RE.fullmatch(block)
    if assignment is None:
        raise ValueError("generated block must contain one PLATFORM_MAP assignment")

    # The generated dictionary is JSON except for URLs written as Python
    # f-strings. Turn those into normal JSON strings before decoding the block.
    # json.loads only reads data; unlike import, eval, or exec it cannot run code.
    def replace_registry_url(match: re.Match[str]) -> str:
        return json.dumps(registry_endpoint + match.group("path"))

    platform_map_json, replacement_count = REGISTRY_URL_RE.subn(
        replace_registry_url, assignment.group("value")
    )
    if replacement_count == 0:
        raise ValueError("PLATFORM_MAP does not contain registry resource URLs")
    try:
        platform_map = json.loads(platform_map_json)
    except json.JSONDecodeError as error:
        raise ValueError(
            "generated PLATFORM_MAP is not in the expected format"
        ) from error
    if not isinstance(platform_map, dict):
        raise ValueError("PLATFORM_MAP must be a dictionary")
    return platform_map


def load_bootstrap_config(path: Path) -> BootstrapConfig:
    source = path.read_text(encoding="utf-8")

    # This narrow match also gives us the exact character range to replace when
    # generating the patch that points ya at the local mirror.
    endpoint_matches = list(REGISTRY_ENDPOINT_RE.finditer(source))
    if len(endpoint_matches) != 1:
        raise ValueError("ya must contain exactly one REGISTRY_ENDPOINT default")
    endpoint_match = endpoint_matches[0]
    registry_endpoint = endpoint_match.group("endpoint").rstrip("/")

    platform_map = extract_platform_map(source, registry_endpoint)
    platforms = platform_map.get("data")
    if not isinstance(platforms, dict):
        raise ValueError("PLATFORM_MAP['data'] must be a dictionary")

    resources: dict[str, BootstrapResource] = {}
    for platform_name, platform in platforms.items():
        if not isinstance(platform_name, str) or not isinstance(platform, dict):
            raise ValueError("PLATFORM_MAP platform entries must be dictionaries")
        md5_value = platform.get("md5")
        if not isinstance(md5_value, str):
            raise ValueError(f"bootstrap md5 for {platform_name} must be a string")
        md5 = md5_value.lower()
        if MD5_RE.fullmatch(md5) is None:
            raise ValueError(f"invalid bootstrap md5 for {platform_name}: {md5!r}")

        urls = platform.get("urls")
        if (
            not isinstance(urls, list)
            or not urls
            or not all(isinstance(url, str) for url in urls)
        ):
            raise ValueError(f"bootstrap urls for {platform_name} must be non-empty")

        # The first URL is the primary source. Resolve its f-string, then use the
        # numeric final path component as the S3 object key.
        source_url = urls[0]
        parsed = urlparse(source_url)
        resource_id = parsed.path.removeprefix("/")
        if not resource_id.isdigit() or parsed.path != f"/{resource_id}":
            raise ValueError(
                f"bootstrap URL for {platform_name} must end in a numeric resource id"
            )
        resource = BootstrapResource(source_url=source_url, md5=md5)
        previous = resources.get(resource_id)
        if previous is not None and previous != resource:
            raise ValueError(f"conflicting bootstrap resource {resource_id}")
        resources[resource_id] = resource

    return BootstrapConfig(
        source=source,
        registry_endpoint=registry_endpoint,
        registry_endpoint_start=endpoint_match.start("endpoint"),
        registry_endpoint_end=endpoint_match.end("endpoint"),
        resources=resources,
    )


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


def localize_mapping(mapping_path: Path, local_base_url: str) -> str:
    data = load_json(mapping_path)
    resources = data.get("resources")
    if not isinstance(resources, dict):
        raise ValueError(f"{mapping_path} does not contain a resources object")

    base = validate_local_base_url(local_base_url)
    for resource_id in list(resources):
        resources[resource_id] = f"{base}/{resource_id}"

    return dump_json(data)


def localize_bootstrap_script(config: BootstrapConfig, local_base_url: str) -> str:
    base = validate_local_base_url(local_base_url)
    endpoint_end = config.registry_endpoint_end

    # Splice at the endpoint match positions. This avoids a broad replacement
    # that could accidentally change the same URL elsewhere in ya.
    return (
        config.source[: config.registry_endpoint_start]
        + base
        + config.source[endpoint_end:]
    )


def file_patch(path: Path, localized_text: str) -> str:
    original = path.read_text(encoding="utf-8").splitlines(keepends=True)
    localized = localized_text.splitlines(keepends=True)
    diff = difflib.unified_diff(
        original,
        localized,
        fromfile=f"a/{path.as_posix()}",
        tofile=f"b/{path.as_posix()}",
    )
    return "".join(diff)


def write_patch(localized_files: list[tuple[Path, str]], patch_out: Path) -> None:
    patch_out.write_text(
        "".join(file_patch(path, text) for path, text in localized_files),
        encoding="utf-8",
    )


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
    bootstrap_total: int,
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
        f"Bootstrap resources in ya: `{bootstrap_total}`",
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
    parser.add_argument("--bootstrap-script", type=Path, default=Path("ya"))
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

    bootstrap = load_bootstrap_config(args.bootstrap_script)
    args.patch_out.parent.mkdir(parents=True, exist_ok=True)
    args.summary_out.parent.mkdir(parents=True, exist_ok=True)

    write_patch(
        [
            (args.mapping, localize_mapping(args.mapping, local_base_url)),
            (
                args.bootstrap_script,
                localize_bootstrap_script(bootstrap, local_base_url),
            ),
        ],
        args.patch_out,
    )

    upload_sources = {
        str(resource_id): str(url) for resource_id, url in resources.items()
    }
    expected_md5: dict[str, str] = {}
    for resource_id, resource in bootstrap.resources.items():
        upload_sources[resource_id] = resource.source_url
        expected_md5[resource_id] = resource.md5

    uploaded: list[str] = []
    skipped: list[str] = []
    if not args.skip_upload:
        s3 = make_s3_client(args.endpoint_url)
        with tempfile.TemporaryDirectory() as tmp:
            tmp_dir = Path(tmp)
            for resource_id, url in sorted(
                upload_sources.items(), key=lambda item: int(item[0])
            ):
                if is_localized_resource_url(resource_id, str(url), local_base_url):
                    skipped.append(resource_id)
                    continue

                existing_md5 = get_existing_md5(s3, args.bucket, resource_id)
                required_md5 = expected_md5.get(resource_id, "")
                if required_md5 and existing_md5 == required_md5:
                    skipped.append(resource_id)
                    continue

                dst = tmp_dir / resource_id
                source_url = validate_source_url(resource_id, str(url))
                md5 = download(source_url, dst, args.download_attempts)
                if required_md5 and md5 != required_md5:
                    raise ValueError(
                        f"bootstrap resource {resource_id} md5 mismatch: "
                        f"expected {required_md5}, got {md5}"
                    )
                if existing_md5 == md5:
                    skipped.append(resource_id)
                    continue
                upload_resource(s3, args.bucket, resource_id, dst, md5)
                uploaded.append(resource_id)

    added, removed = resource_delta(args.mapping, args.base_mapping)
    write_summary(
        args.summary_out,
        total=len(resources),
        bootstrap_total=len(bootstrap.resources),
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
