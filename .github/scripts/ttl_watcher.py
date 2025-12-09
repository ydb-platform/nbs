import argparse
import boto3
import logging
import re
from urllib.parse import urlparse
from botocore.exceptions import ClientError


def set_logging_level(verbosity):
    level = max(10, 40 - 10 * verbosity)
    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")


def ttl_to_days(ttl_str):
    seconds = ttl_to_seconds(ttl_str)
    return max(1, (seconds + 86399) // 86400)


def ttl_to_seconds(ttl_str):
    pattern = (
        r"((?P<days>\d+)d)?((?P<hours>\d+)h)?((?P<minutes>\d+)m)?((?P<seconds>\d+)s)?"
    )
    matches = re.match(pattern, ttl_str)
    time_parts = {
        name: int(value) for name, value in matches.groupdict(default="0").items()
    }
    total_seconds = (
        time_parts["days"] * 86400  # noqa: W503
        + time_parts["hours"] * 3600  # noqa: W503
        + time_parts["minutes"] * 60  # noqa: W503
        + time_parts["seconds"]  # noqa: W503
    )
    return total_seconds


def parse_s3_path(s3_path):
    parsed_url = urlparse(s3_path)
    if parsed_url.scheme != "s3" or not parsed_url.netloc:
        raise ValueError("URL must be an S3 URL (s3://bucket[/prefix])")
    return parsed_url.netloc, parsed_url.path.lstrip("/")


def find_target_directories(
    s3_client, bucket, prefix, target_dirs, current_depth=0, max_depth=10
):
    if current_depth > max_depth:
        return set()
    paginator = s3_client.get_paginator("list_objects_v2")
    found_directories = set()
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for common_prefix in page.get("CommonPrefixes", []):
            subdir = common_prefix.get("Prefix")
            short_directory = subdir.rstrip("/").split("/")[-1]
            if short_directory in target_dirs:
                found_directories.add(subdir)
            else:
                # Recursively list deeper directories
                deeper_dirs = find_target_directories(
                    s3_client, bucket, subdir, target_dirs, current_depth + 1, max_depth
                )
                found_directories.update(deeper_dirs)
    return found_directories


def rule_parser(rule):
    return (
        rule.get("ID", "no_id"),
        rule.get("Filter", {}).get("Prefix", "no_prefix"),
        rule.get("Expiration", {}).get("Days", 0),
    )


def print_rule(rule):
    rule_id, rule_prefix, rule_ttl = rule_parser(rule)
    logging.info(
        f"Rule with id: {rule_id}, for prefix: {rule_prefix}, TTL set to {rule_ttl} days"
    )


def update_lifecycle_policy(
    s3_client, bucket_name, directories, ttl_config, force, apply_changes
):
    existing_rules = []
    try:
        response = s3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
        existing_rules = response["Rules"]
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchLifecycleConfiguration":
            logging.error(f"Failed to retrieve lifecycle configuration: {e}")
            raise

    # Prepare the updated rules list
    logging.info("Existing rules start")
    updated_rules = []
    for rule in existing_rules:
        # Remove or modify existing rules based on some criteria, if necessary
        if not force:
            print_rule(rule)
            updated_rules.append(rule)  # Keep existing rules if not forcing updates
    logging.info("Existing rules list end")
    # Add or update rules for the specified directories
    for directory in directories:
        short_directory = directory.rstrip("/").split("/")[-1]
        days = ttl_to_days(
            ttl_config.get(short_directory, ttl_config.get("default", "30d"))
        )
        rule_id = f"Expire_{directory.replace('/', '_')}"

        rule_exists = any(rule["ID"] == rule_id for rule in existing_rules)
        if rule_exists and not force:
            logging.info(f"Skipping existing rule {rule_id} without --force.")
            continue

        rule = {
            "ID": rule_id,
            "Filter": {"Prefix": directory},
            "Status": "Enabled",
            "Expiration": {"Days": days},
        }
        logging.info(f"Updated rule for {directory}, with TTL: {days}")
        updated_rules.append(rule)  # Append new or updated rule

    # Apply the updated rules back to the bucket
    if apply_changes:
        try:
            s3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name, LifecycleConfiguration={"Rules": updated_rules}
            )
            logging.info("Updated bucket lifecycle configuration.")
        except ClientError as e:
            logging.error(f"Failed to update lifecycle configuration: {e}")
    else:
        logging.info(
            "Dry run: Bucket lifecycle configuration not updated. Updated list of rules would be:"
        )
        for rule in updated_rules:
            print_rule(rule)
        logging.info("Dry run: list of rules end")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update S3 object lifecycle based on TTL."
    )
    parser.add_argument("s3_path", help="S3 path in the format s3://bucket/prefix")
    parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="Increase verbosity level"
    )
    parser.add_argument(
        "--ttl",
        action="append",
        default=[],
        help="TTL settings for prefixes, e.g., 'dir=7d', rounded up to nearest full day",
    )
    parser.add_argument(
        "--apply", action="store_true", help="Apply changes, without it is a dry run"
    )
    parser.add_argument(
        "--force", action="store_true", help="Force update of lifecycle rules"
    )
    args = parser.parse_args()

    set_logging_level(args.verbose)

    bucket_name, base_prefix = parse_s3_path(args.s3_path)

    ttl_config = {"default": "30d", "test_data": "7d", "ya_archive": "7d"}
    for ttl_setting in args.ttl:
        key, value = ttl_setting.split("=")
        ttl_config[key] = value

    target_dirs = {
        "test_reports",
        "test_data",
        "test_logs",
        "summary",
        "logs",
        "build_logs",
        "artifacts",
        "ya_archive",
    }

    s3_client = boto3.client("s3")
    directories = find_target_directories(
        s3_client, bucket_name, base_prefix, target_dirs
    )

    update_lifecycle_policy(
        s3_client, bucket_name, directories, ttl_config, args.force, args.apply
    )
