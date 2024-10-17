#!/usr/bin/env python3
import boto3
import re
import os
import logging
import argparse
import json
from urllib.parse import unquote_plus, urlparse
from concurrent.futures import ThreadPoolExecutor
from jinja2 import Environment, FileSystemLoader
from datetime import datetime, timedelta, timezone


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


def sizeof_fmt(num, suffix="B"):
    """Convert file size to a human-readable format."""
    for unit in ["", "K", "M", "G", "T", "P", "E", "Z"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def list_files(client, bucket, prefix):
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        dirs = page.get("CommonPrefixes", [])
        files = [
            content
            for content in page.get("Contents", [])
            if not content["Key"].endswith("/")
        ]
        return dirs, files


def list_files_for_deletion(s3, bucket, prefix, ttl_config, now):
    logging.debug(f"Looking for files in {bucket}/{prefix} for deletion")
    files_to_delete = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        dirs = page.get("CommonPrefixes", [])
        files = [
            content
            for content in page.get("Contents", [])
            if not content["Key"].endswith("/")
        ]
        for f in files:
            key = f["Key"]
            last_modified = f["LastModified"]
            ttl = get_ttl_for_prefix(prefix, ttl_config)
            if ttl and now - last_modified > timedelta(seconds=ttl):
                logging.info(
                    f"File {key} has expired and will be deleted {now}-{last_modified} > {timedelta(seconds=ttl)}"
                )
                files_to_delete.add(key)
            # we are deleting them because if we will make proper logic to delete them,
            # it will add a lot of complexity and we will add them regardless in the future
            if key.endswith("/index.html"):
                files_to_delete.add(key)
                logging.info(
                    f"Found index.html file in {bucket}/{prefix}, will delete preemptively"
                )

        return [d["Prefix"] for d in dirs], files_to_delete


def generate_absolute_url(bucket, key, website_suffix):
    return f"https://{bucket}.{website_suffix}/{key}"


def get_ttl_for_prefix(prefix, ttl_config):
    logging.debug(f"Looking for TTL for prefix {prefix}")
    for ttl_dir in ttl_config:
        if ttl_dir in prefix.split("/"):
            logging.debug(f"TTL found for prefix {prefix}: {ttl_config[ttl_dir]}")
            return ttl_config[ttl_dir]
    logging.debug(f"No TTL found for prefix {prefix}, skipping")
    return False


def generate_index_html(
    bucket, files, dirs, current_prefix, website_suffix, ttl_config, now
):
    # Setup Jinja environment
    env = Environment(
        loader=FileSystemLoader(os.path.join(os.path.dirname(__file__), "templates"))
    )
    template = env.get_template("index.html")

    # Prepare data
    entries = []
    if current_prefix != "":
        parent_dir = os.path.dirname(current_prefix.rstrip("/"))
        if parent_dir != "":
            parent_dir += "/"
        entries.append(
            {
                "name": "../",
                "url": generate_absolute_url(bucket, parent_dir, website_suffix)
                + "index.html",
                "type": "directory",
                "date": "",
                "size": "",  # Directories don't have a size
            }
        )

    ttl = get_ttl_for_prefix(current_prefix, ttl_config)

    for d in dirs:
        dir_name = d["Prefix"]
        if dir_name != current_prefix:
            dir_url = (
                generate_absolute_url(bucket, unquote_plus(dir_name), website_suffix)
                + "index.html"
            )
            entries.append(
                {
                    "name": os.path.basename(dir_name[:-1]) + "/",
                    "url": dir_url,
                    "type": "directory",
                    "date": "",
                    "size": "",  # Directories don't have a size
                }
            )
    for f in files:
        file_key = f["Key"]
        if file_key != current_prefix + "index.html":
            file_url = generate_absolute_url(
                bucket, unquote_plus(file_key), website_suffix
            )
            file_date = datetime.fromtimestamp(f["LastModified"].timestamp()).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            expiry_date = f["LastModified"] + timedelta(seconds=ttl)
            expiry_date_str = expiry_date.strftime("%Y-%m-%d %H:%M:%S")
            file_size = sizeof_fmt(f["Size"])
            entries.append(
                {
                    "name": os.path.basename(file_key),
                    "url": file_url,
                    "type": "file",
                    "expiry_date": expiry_date_str if ttl else "",
                    "is_expired": now > expiry_date if ttl else False,
                    "date": file_date,
                    "size": file_size,
                }
            )

    # Render template
    current_timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
    return template.render(
        entries=entries, directory=current_prefix, timestamp=current_timestamp
    )


def upload_index_html(s3, bucket, prefix, html_content):
    try:
        s3.put_object(
            Body=html_content,
            Bucket=bucket,
            Key=os.path.join(prefix, "index.html"),
            ContentType="text/html",
        )
        logging.info(f"Successfully uploaded index.html to {bucket}/{prefix}")
    except Exception as e:
        logging.error(f"Error uploading index.html to {bucket}/{prefix}: {e}")


def process_directory(
    s3, bucket, prefix, website_suffix, ttl_config, now, apply_changes
):
    dirs, files = list_files(s3, bucket, prefix)
    # Filter out the 'index.html' file if present
    files_without_index = [
        file for file in files if not file["Key"].endswith("index.html")
    ]

    if not dirs and not files_without_index:
        # The directory is empty (or only contains 'index.html'), so delete 'index.html' if it exists
        try:
            if apply_changes:
                s3.delete_object(Bucket=bucket, Key=os.path.join(prefix, "index.html"))
                logging.info(f"Removed 'index.html' from empty directory: {prefix}")
            else:
                logging.info(
                    f"Dry-run: Would remove 'index.html' from empty directory: {prefix}"
                )
        except s3.exceptions.NoSuchKey:
            # 'index.html' does not exist, no action needed
            pass
    else:
        # The directory is not empty, generate/update 'index.html'
        html_content = generate_index_html(bucket, files, dirs, prefix, ttl_config, now)
        if apply_changes:
            upload_index_html(s3, bucket, prefix, html_content)
        else:
            logging.info(f"Dry-run: Would upload index.html to {bucket}/{prefix}")
    return [d["Prefix"] for d in dirs]


def batch_delete_files(s3, bucket, files_to_delete, batch_size, apply_changes):
    while files_to_delete:
        batch = files_to_delete[:batch_size]
        delete_objects = [{"Key": key} for key in batch]

        if apply_changes:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": delete_objects})
            logging.info(f"Deleted {len(batch)} objects")
        else:
            logging.info(
                f"Dry-run: Would delete {len(batch)} objects: {delete_objects}"
            )

        files_to_delete = files_to_delete[batch_size:]


def main(
    s3,
    bucket_name,
    base_prefix,
    website_suffix,
    ttl_config,
    now,
    max_workers,
    remove_expired,
    generate_indexes,
    batch_size,
    apply_changes,
):
    if remove_expired:
        files_for_deletion = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    list_files_for_deletion,
                    s3,
                    bucket_name,
                    base_prefix,
                    ttl_config,
                    now,
                )
            }
            while futures:
                done, futures = futures, set()
                for future in done:
                    new_prefixes, files_for_deletion_result = future.result()
                    files_for_deletion.extend(files_for_deletion_result)
                    for new_prefix in new_prefixes:
                        futures.add(
                            executor.submit(
                                list_files_for_deletion,
                                s3,
                                bucket_name,
                                new_prefix,
                                ttl_config,
                                now,
                            )
                        )

        logging.info(f"Files for deletion: {files_for_deletion}")
        batch_delete_files(
            s3, bucket_name, files_for_deletion, batch_size, apply_changes
        )

    if generate_indexes:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    process_directory,
                    s3,
                    bucket_name,
                    base_prefix,
                    website_suffix,
                    ttl_config,
                    now,
                    apply_changes,
                )
            }
            while futures:
                done, futures = futures, set()
                for future in done:
                    new_prefixes = future.result()
                    for new_prefix in new_prefixes:
                        futures.add(
                            executor.submit(
                                process_directory,
                                s3,
                                bucket_name,
                                new_prefix,
                                website_suffix,
                                ttl_config,
                                now,
                                apply_changes,
                            )
                        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Update S3 object lifecycle based on TTL."
    )
    parser.add_argument("s3_path", help="S3 path in the format s3://bucket/prefix")
    parser.add_argument(
        "-v", "--verbose", action="count", default=2, help="Increase verbosity level"
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
        "--website-suffix",
        default=os.environ.get("S3_WEBSITE_SUFFIX", "website.nemax.nebius.cloud"),
        help="Suffix for website domain",
    )

    parser.add_argument(
        "--remove-expired",
        action="store_true",
        help="Remove files that have expired",
        default=False,
    )

    parser.add_argument(
        "--generate-indexes",
        action="store_true",
        help="Generate index.html files for directories",
        default=False,
    )

    parser.add_argument(
        "--batch-size", type=int, default=32, help="Batch size for deletion"
    )

    parser.add_argument(
        "--max-workers",
        type=int,
        default=4,
        help="Maximum number of workers for threads",
    )

    args = parser.parse_args()
    set_logging_level(args.verbose)
    bucket_name, base_prefix = parse_s3_path(args.s3_path)

    s3 = boto3.client("s3")
    now = datetime.now(timezone.utc)

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

    ttl_config = {"default": "30d", "test_data": "7d", "ya_archive": "7d"}
    for ttl_setting in args.ttl:
        key, value = ttl_setting.split("=")
        ttl_config[key] = value

    ttl_config_converted = {}
    for d in target_dirs:
        ttl_config_converted[d] = ttl_to_seconds(
            ttl_config.get(d, ttl_config["default"])
        )

    configuration = f"bucket_name={bucket_name}, base_prefix={base_prefix}, now={now}, "
    configuration += f"remove_expired={args.remove_expired}, generate_indexes={args.generate_indexes}, "
    configuration += f"batch_size={args.batch_size} max_workers={args.max_workers} apply={args.apply}"
    logging.info(f"Script launches with the following configuration: {configuration}")
    logging.info(
        f"TTL configuration: \n{json.dumps(ttl_config_converted, indent=True)}"
    )
    main(
        s3,
        bucket_name,
        base_prefix,
        args.website_suffix,
        ttl_config_converted,
        now,
        args.max_workers,
        args.remove_expired,
        args.generate_indexes,
        args.batch_size,
        args.apply,
    )
