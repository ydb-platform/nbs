import os
import math
import logging
import argparse
import requests
from dataclasses import dataclass
import datetime

SENSITIVE_DATA_VALUES = {}
if os.environ.get("GITHUB_TOKEN"):
    SENSITIVE_DATA_VALUES["github_token"] = os.environ.get("GITHUB_TOKEN")
if os.environ.get("VM_USER_PASSWD"):
    SENSITIVE_DATA_VALUES["passwd"] = os.environ.get("VM_USER_PASSWD")


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


def setup_logger(loglevel=logging.INFO):
    formatter = MaskingFormatter("%(asctime)s: %(levelname)s: %(message)s")
    console_handler = logging.StreamHandler()
    console_handler.setLevel(loglevel)
    console_handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.setLevel(loglevel)
    logger.addHandler(console_handler)
    return logger


def github_output(
    logger: logging.Logger, key: str, value: str, is_secret: bool = False
):
    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        with open(output_path, "a") as fp:
            fp.write(f"{key}={value}\n")
    logger.info('echo "%s=%s" >> $GITHUB_OUTPUT', key, "******" if is_secret else value)


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
