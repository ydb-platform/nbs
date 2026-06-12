import os
import math
import json
import logging
import argparse
import requests
import re
import asyncio
import functools
import time
from dataclasses import dataclass
import datetime
from typing import Callable, List, Tuple
from urllib.error import HTTPError
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

SENSITIVE_DATA_VALUES = {}
if os.environ.get("GITHUB_TOKEN"):
    SENSITIVE_DATA_VALUES["github_token"] = os.environ.get("GITHUB_TOKEN")
if os.environ.get("VM_USER_PASSWD"):
    SENSITIVE_DATA_VALUES["passwd"] = os.environ.get("VM_USER_PASSWD")

COMPONENTS: List[Tuple[str, str, str]] = [
    ("blockstore", "cloud/blockstore/apps/", "cloud/blockstore/"),
    ("filestore", "cloud/filestore/apps/", "cloud/filestore/"),
    ("disk_manager", "cloud/disk_manager/", "cloud/disk_manager/"),
    ("tasks", "cloud/tasks/", "cloud/tasks/"),
    ("storage", "cloud/storage/", "cloud/storage/"),
]

SAN_COMPONENTS = {"blockstore", "filestore", "storage"}
SAN_TYPES = ("asan", "tsan", "msan", "ubsan")

TEST_TYPE_REGULAR = "unittest,clang_tidy,gtest,py3test,py2test,pytest,flake8,black,py2_flake8,go_test,gofmt"
TEST_TYPE_SAN = "unittest,clang_tidy,gtest,py3test,py2test,pytest"

SAN_PRESET = {
    "asan": ("release-asan", "-asan"),
    "tsan": ("release-tsan", "-tsan"),
    "msan": ("release-msan", "-msan"),
    "ubsan": ("release-ubsan", "-ubsan"),
}

SAN_PRESETS = {"release-asan", "release-tsan", "release-msan", "release-ubsan"}
SAN_SUFFIX = {"asan": "-asan", "tsan": "-tsan", "msan": "-msan", "ubsan": "-ubsan"}
SAN_PRESET_BY_SAN = {
    "asan": "release-asan",
    "tsan": "release-tsan",
    "msan": "release-msan",
    "ubsan": "release-ubsan",
}


def get_build_preset_from_workflow_name(workflow_name: str) -> str | None:
    normalized = workflow_name.strip().lower()
    if normalized in ("nightly.yaml", "nightly build"):
        return "relwithdebinfo"

    match = re.fullmatch(
        r"nightly(?: build)? \((?P<san>asan|tsan|msan|ubsan)\)", normalized
    )
    if match is not None:
        return SAN_PRESET_BY_SAN[match.group("san")]

    match = re.fullmatch(r"nightly-(?P<san>asan|tsan|msan|ubsan)\.yaml", normalized)
    if match is not None:
        return SAN_PRESET_BY_SAN[match.group("san")]

    return None


def get_s3_website_origin(
    bucket: str | None = None, website_suffix: str | None = None
) -> str:
    bucket = (bucket or os.environ.get("S3_BUCKET") or "").strip()
    website_suffix = (
        website_suffix or os.environ.get("S3_WEBSITE_SUFFIX") or ""
    ).strip()
    if not bucket or not website_suffix:
        return ""
    return f"https://{bucket}.{website_suffix}"


def get_s3_report_uri(path: str, bucket: str | None = None) -> str:
    bucket = (bucket or os.environ.get("S3_BUCKET") or "").strip()
    if not bucket or not path:
        return ""
    return f"s3://{bucket}/{path}"


def get_s3_report_url(
    path: str,
    bucket: str | None = None,
    website_suffix: str | None = None,
) -> str:
    origin = get_s3_website_origin(bucket=bucket, website_suffix=website_suffix)
    if not origin or not path:
        return ""
    return f"{origin}/{quote(path, safe='/')}"


def get_s3_workflow_reports_path(
    *,
    repository: str,
    workflow_file: str,
    run_id: int | str,
    run_attempt: int | str,
    relative_path: str = "",
) -> str:
    return (
        f"reports/{repository}/{workflow_file}/{run_id}/{run_attempt}/"
        f"{relative_path.lstrip('/')}"
    )


def get_run_url() -> str:
    return f"https://github.com/{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}"


def fetch_jobs() -> list[dict]:
    owner, repo = os.environ["GITHUB_REPOSITORY"].split("/", 1)
    run_id = os.environ["GITHUB_RUN_ID"]
    run_attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    url = (
        f"https://api.github.com/repos/{owner}/{repo}/actions/runs/"
        f"{run_id}/attempts/{run_attempt}/jobs?per_page=100"
    )
    request = Request(
        url,
        headers={
            "Authorization": f"Bearer {os.environ['GITHUB_TOKEN']}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urlopen(request) as response:
        payload = json.load(response)
    return payload.get("jobs", [])


def job_name_matches(expected_name: str, actual_name: str) -> bool:
    if actual_name == expected_name:
        return True

    reusable_prefix = f"/ {expected_name}"
    if actual_name.endswith(reusable_prefix):
        return True

    return False


def find_current_job_url(current_job_name: str, runner_name: str) -> str:
    try:
        jobs = fetch_jobs()
    except HTTPError:
        return get_run_url()

    matching_jobs = [
        job
        for job in jobs
        if job_name_matches(current_job_name, job.get("name", ""))
        and job.get("status") in ("queued", "in_progress", "completed")
    ]

    if runner_name:
        for job in matching_jobs:
            if job.get("runner_name") != runner_name:
                continue
            html_url = job.get("html_url")
            if html_url:
                return html_url

    for job in matching_jobs:
        html_url = job.get("html_url")
        if html_url:
            return html_url

    return get_run_url()


DEFAULT_BUILD_TARGET = "cloud/blockstore/apps/,cloud/filestore/apps/,cloud/disk_manager/,cloud/tasks/,cloud/storage/"
DEFAULT_TEST_TARGET = (
    "cloud/blockstore/,cloud/filestore/,cloud/disk_manager/,cloud/tasks/,cloud/storage/"
)

GITHUB_API_RETRY_ATTEMPTS = 3
GITHUB_API_RETRY_INTERVAL_SEC = 5
GITHUB_API_TIMEOUT_SEC = 30
GITHUB_RUNNER_LATEST_VERSION = "latest"
GITHUB_RUNNER_LATEST_RELEASE_URL = (
    "https://api.github.com/repos/actions/runner/releases/latest"
)
GITHUB_RUNNER_RELEASE_BY_TAG_URL = (
    "https://api.github.com/repos/actions/runner/releases/tags/v{version}"
)


@dataclass(frozen=True)
class GithubRunnerRelease:
    version: str
    sha256_by_arch: dict[str, str]


def truthy(v: str | None) -> bool:
    return (v or "").strip().lower() == "true"


def csv_join(parts: List[str]) -> str:
    return ",".join(parts)


def split_csv(csv_value: str) -> List[str]:
    return [p.strip() for p in csv_value.split(",") if p.strip()]


def json_array(items: List[str]) -> str:
    return json.dumps(items, separators=(",", ":"))


def json_obj(obj) -> str:
    return json.dumps(obj, separators=(",", ":"))


def retry(
    attempts: int,
    interval_sec: int,
    retry_exceptions: tuple[type[BaseException], ...] = (Exception,),
    retry_result: Callable[[object], bool] | None = None,
    attempt_arg: str | None = None,
    on_final_exception: Callable[[BaseException], None] | None = None,
) -> callable:
    def decorator(func: callable) -> callable:
        def build_call_kwargs(kwargs: dict, attempt: int) -> dict:
            call_kwargs = dict(kwargs)
            if attempt_arg:
                call_kwargs[attempt_arg] = attempt - 1
            return call_kwargs

        def should_retry_result(result) -> bool:
            return retry_result is not None and retry_result(result)

        logger = logging.getLogger(func.__module__)
        operation = func.__name__

        def log_final_exception(exception: BaseException) -> None:
            logger.error(
                "%s failed after %d attempts: %s",
                operation,
                attempts,
                exception,
            )

        def log_retry_exception(attempt: int, exception: BaseException) -> None:
            logger.warning(
                "%s failed on attempt %d/%d: %s. Retrying in %d seconds",
                operation,
                attempt,
                attempts,
                exception,
                interval_sec,
            )
            logger.debug("%s retryable failure details", operation, exc_info=True)

        def log_retry_result(attempt: int) -> None:
            logger.info(
                "%s returned retryable result on attempt %d/%d. Retrying in %d seconds",
                operation,
                attempt,
                attempts,
                interval_sec,
            )

        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                for attempt in range(1, attempts + 1):
                    try:
                        result = await func(*args, **build_call_kwargs(kwargs, attempt))
                    except retry_exceptions as e:
                        if attempt == attempts:
                            log_final_exception(e)
                            if on_final_exception:
                                on_final_exception(e)
                            raise

                        log_retry_exception(attempt, e)
                        await asyncio.sleep(interval_sec)
                        continue

                    if not should_retry_result(result):
                        return result

                    if attempt == attempts:
                        logger.error(
                            "%s did not produce expected result after %d attempts",
                            operation,
                            attempts,
                        )
                        return result

                    log_retry_result(attempt)
                    await asyncio.sleep(interval_sec)

            return async_wrapper

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, attempts + 1):
                try:
                    result = func(*args, **build_call_kwargs(kwargs, attempt))
                except retry_exceptions as e:
                    if attempt == attempts:
                        log_final_exception(e)
                        if on_final_exception:
                            on_final_exception(e)
                        raise

                    log_retry_exception(attempt, e)
                    time.sleep(interval_sec)
                    continue

                if not should_retry_result(result):
                    return result

                if attempt == attempts:
                    logger.error(
                        "%s did not produce expected result after %d attempts",
                        operation,
                        attempts,
                    )
                    return result

                log_retry_result(attempt)
                time.sleep(interval_sec)

        return wrapper

    return decorator


def fetch_repo_variable(github_client, github_repository: str, variable_name: str):
    repo = github_client.get_repo(github_repository)
    return repo, repo.get_variable(variable_name)


def format_github_response_debug(response: requests.Response) -> str:
    content_type = response.headers.get("content-type", "<missing>")
    body_preview = response.text[:1000].replace("\n", "\\n")
    return (
        f"status={response.status_code}, reason={response.reason!r}, "
        f"content_type={content_type!r}, body_preview={body_preview!r}"
    )


def normalize_github_runner_version(version: str) -> str:
    version = (version or "").strip()
    if not version:
        raise ValueError("GitHub runner version is empty")
    return version.removeprefix("v")


def github_api_headers(github_token: str | None = None) -> dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-Github-Api-Version": "2022-11-28",
    }
    if github_token:
        headers["Authorization"] = f"Bearer {github_token}"
    return headers


def extract_github_runner_sha256_from_body(body: str, platform: str) -> str:
    pattern = (
        rf"<!--\s*BEGIN SHA {re.escape(platform)}\s*-->\s*"
        r"([0-9a-fA-F]{64})"
        rf"\s*<!--\s*END SHA {re.escape(platform)}\s*-->"
    )
    match = re.search(pattern, body or "")
    if not match:
        raise ValueError(
            f"GitHub runner release body is missing SHA-256 marker for {platform}"
        )
    return match.group(1).lower()


def extract_github_runner_release(payload: dict) -> GithubRunnerRelease:
    tag_name = payload.get("tag_name")
    if not tag_name:
        raise ValueError("GitHub runner release response is missing tag_name")

    version = normalize_github_runner_version(tag_name)
    body = payload.get("body", "")
    sha256_by_arch = {}
    platforms_by_arch = {
        "x64": "linux-x64",
        "arm64": "linux-arm64",
    }
    for arch, platform in platforms_by_arch.items():
        asset_name = f"actions-runner-linux-{arch}-{version}.tar.gz"
        matching_asset = next(
            (
                asset
                for asset in payload.get("assets", [])
                if asset.get("name") == asset_name
            ),
            None,
        )
        if matching_asset is None:
            raise ValueError(
                f"GitHub runner release {version} is missing asset {asset_name}"
            )

        sha256_by_arch[arch] = extract_github_runner_sha256_from_body(body, platform)

    return GithubRunnerRelease(version=version, sha256_by_arch=sha256_by_arch)


@retry(
    attempts=GITHUB_API_RETRY_ATTEMPTS,
    interval_sec=GITHUB_API_RETRY_INTERVAL_SEC,
    retry_exceptions=(requests.exceptions.RequestException, ValueError),
)
def get_github_runner_release(
    version: str, github_token: str | None = None
) -> GithubRunnerRelease:
    normalized_version = normalize_github_runner_version(version)
    response = requests.get(
        GITHUB_RUNNER_RELEASE_BY_TAG_URL.format(version=normalized_version),
        headers=github_api_headers(github_token),
        timeout=GITHUB_API_TIMEOUT_SEC,
    )
    try:
        result = response.json()
    except ValueError as e:
        raise ValueError(
            "Failed to parse GitHub runner release response "
            f"({format_github_response_debug(response)}): {e}"
        ) from None

    if not response.ok:
        raise ValueError(
            "GitHub runner release request failed "
            f"({format_github_response_debug(response)}): {result}"
        )

    return extract_github_runner_release(result)


@retry(
    attempts=GITHUB_API_RETRY_ATTEMPTS,
    interval_sec=GITHUB_API_RETRY_INTERVAL_SEC,
    retry_exceptions=(requests.exceptions.RequestException, ValueError),
)
def get_latest_github_runner_release(
    github_token: str | None = None,
) -> GithubRunnerRelease:
    response = requests.get(
        GITHUB_RUNNER_LATEST_RELEASE_URL,
        headers=github_api_headers(github_token),
        timeout=GITHUB_API_TIMEOUT_SEC,
    )
    try:
        result = response.json()
    except ValueError as e:
        raise ValueError(
            "Failed to parse GitHub runner latest release response "
            f"({format_github_response_debug(response)}): {e}"
        ) from None

    if not response.ok:
        raise ValueError(
            "GitHub runner latest release request failed "
            f"({format_github_response_debug(response)}): {result}"
        )

    return extract_github_runner_release(result)


def get_latest_github_runner_version(github_token: str | None = None) -> str:
    return get_latest_github_runner_release(github_token).version


def resolve_github_runner_version(version: str, github_token: str | None = None) -> str:
    return resolve_github_runner_release(version, github_token).version


def resolve_github_runner_release(
    version: str, github_token: str | None = None
) -> GithubRunnerRelease:
    if (version or "").strip().lower() in ("", GITHUB_RUNNER_LATEST_VERSION):
        return get_latest_github_runner_release(github_token)
    return get_github_runner_release(version, github_token)


def vm_suffix_for_component(component: str) -> str:
    return f"-{component}"


def is_san_preset(build_preset: str) -> bool:
    return (build_preset or "").strip() in SAN_PRESETS


def san_from_preset(build_preset: str) -> str | None:
    preset = (build_preset or "").strip()
    for san, p in SAN_PRESET_BY_SAN.items():
        if preset == p:
            return san
    return None


class KeyValueAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):  # noqa: U100
        kv_dict = {}
        for item in values.split(","):
            key, value = item.split("=")
            kv_dict[key] = value
        setattr(namespace, self.dest, kv_dict)


class MaskingFormatter(logging.Formatter):
    @staticmethod
    def mask_sensitive_data(msg):
        for name, val in SENSITIVE_DATA_VALUES.items():
            if val:
                msg = msg.replace(val, f"[{name}=***]")
        return msg

    def format(self, record):
        original = super().format(record)
        return self.mask_sensitive_data(original)


def setup_logger(
    loglevel=logging.INFO,
    name: str | None = None,
    fmt: str = "%(asctime)s: %(levelname)s: %(message)s",
):
    formatter = MaskingFormatter(fmt)
    logger = logging.getLogger(name)
    logger.setLevel(loglevel)
    logger.propagate = False

    if not logger.handlers:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(loglevel)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
    else:
        for handler in logger.handlers:
            handler.setLevel(loglevel)
            handler.setFormatter(formatter)

    return logger


def set_logging_level(
    verbosity: int, fmt: str = "%(asctime)s - %(levelname)s - %(message)s"
):
    level = max(10, 40 - 10 * verbosity)
    return setup_logger(level, fmt=fmt)


def write_to_file_from_env(
    logger: logging.Logger,
    key: str,
    value: str,
    env_var: str,
    is_secret: bool = False,
):
    output_path = os.environ.get(env_var)
    if output_path:
        with open(output_path, "a") as fp:
            fp.write(f"{key}={value}\n")
    logger.info(
        'echo "%s=%s" >> $%s',
        key,
        "******" if is_secret else value,
        env_var,
    )


def github_output(
    logger: logging.Logger, key: str, value: str, is_secret: bool = False
):
    write_to_file_from_env(logger, key, value, "GITHUB_OUTPUT", is_secret)


def github_env(logger: logging.Logger, key: str, value: str, is_secret: bool = False):
    write_to_file_from_env(logger, key, value, "GITHUB_ENV", is_secret)


def ttl_to_days(ttl_str: str) -> int:
    seconds = ttl_to_seconds(ttl_str)
    return max(1, (seconds + 86399) // 86400)


def ttl_to_seconds(ttl_str: str) -> int:
    pattern = (
        r"((?P<days>\d+)d)?((?P<hours>\d+)h)?((?P<minutes>\d+)m)?((?P<seconds>\d+)s)?"
    )
    matches = re.match(pattern, ttl_str)
    time_parts = {
        name: int(value) for name, value in matches.groupdict(default="0").items()
    }
    return (
        time_parts["days"] * 86400  # noqa: W503
        + time_parts["hours"] * 3600  # noqa: W503
        + time_parts["minutes"] * 60  # noqa: W503
        + time_parts["seconds"]  # noqa: W503
    )


def parse_s3_path(s3_path: str) -> tuple[str, str]:
    parsed_url = urlparse(s3_path)
    if parsed_url.scheme != "s3" or not parsed_url.netloc:
        raise ValueError("URL must be an S3 URL (s3://bucket[/prefix])")
    return parsed_url.netloc, parsed_url.path.lstrip("/")


def convert_size(size_bytes):
    if size_bytes == 0:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = math.floor(math.log(size_bytes, 1024))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"


def classify_runner(labels):
    if "self-hosted" in labels:
        if "runner_light" in labels:
            return "runner_light"
        elif "runner_heavy" in labels:
            return "runner_heavy"
        else:
            return "runner_none"
    else:
        image_labels = [label for label in labels if label not in ("Linux", "X64")]
        return f"{image_labels[0]}" if image_labels else "unknown"


def compact_job_name(job_name: str) -> str:
    """Convert a job name to a compact format."""
    if job_name.startswith("Build and test"):
        return job_name.replace("Build and test", "").strip()
    if "(" in job_name:
        return job_name.split("(")[0].strip()
    if ".yaml" in job_name or ".yml" in job_name:
        return job_name.replace(".yaml", "").replace(".yml", "").strip()
    return job_name


def compact_workflow_name(workflow_name: str) -> str:
    """Convert a workflow name to a compact format."""
    if "(" in workflow_name:
        return workflow_name.split("(")[0].strip()
    return workflow_name


def date_to_hms(date: datetime.datetime) -> str:
    """Convert a datetime object to a formatted string."""
    now = datetime.datetime.now(datetime.timezone.utc)
    age = now - date

    age_days = age.days
    age_hours, remainder = divmod(age.seconds, 3600)
    age_minutes, _ = divmod(remainder, 60)

    if age_days > 0:
        return f"{age_days}d{age_hours}h{age_minutes}m"
    elif age_hours > 0:
        return f"{age_hours}h{age_minutes}m"
    else:
        return f"{age_minutes}m"


@dataclass
class Job:
    workflow: str
    id: int
    run_id: int
    name: str
    runner_name: str
    runner_type: str
    created_at: datetime.datetime
    completed_at: datetime.datetime
    started_at: datetime.datetime
    conclusion: str
    status: str
    labels: list[str] = None


def get_jobs_raw(token, repo_full_name, run_id) -> list[Job]:
    result = []
    url = f"https://api.github.com/repos/{repo_full_name}/actions/runs/{run_id}/jobs"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    jobs = response.json()["jobs"]

    for job in jobs:
        for t in ["created_at", "started_at", "completed_at"]:
            if job[t] is not None:
                job[t] = datetime.datetime.fromisoformat(job[t].replace("Z", "+00:00"))

        result.append(
            Job(
                workflow=job["name"],
                id=job["id"],
                run_id=run_id,
                name=job["name"],
                runner_name=job["runner_name"],
                runner_type=classify_runner(job.get("labels", [])),
                created_at=job.get("created_at"),
                started_at=job.get("started_at"),
                completed_at=job.get("completed_at"),
                conclusion=job["conclusion"],
                status=job["status"],
                labels=job.get("labels", []),
            )
        )
    return result
