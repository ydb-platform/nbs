#!/usr/bin/env python3
import boto3
import sys
import os
import logging
from urllib.parse import unquote_plus, urlparse
from concurrent.futures import ThreadPoolExecutor
from jinja2 import Environment, FileSystemLoader
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s"
)


def sizeof_fmt(num, suffix="B"):
    """Converts file size to a human-readable format."""
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


def generate_absolute_url(bucket, key):
    return f"https://{bucket}.website.nemax.nebius.cloud/{key}"


def generate_index_html(bucket, files, dirs, current_prefix):
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
                "url": generate_absolute_url(bucket, parent_dir),
                "type": "directory",
                "date": "",
                "size": "",  # Directories don't have a size
            }
        )
    for d in dirs:
        dir_name = d["Prefix"]
        if dir_name != current_prefix:
            dir_url = generate_absolute_url(bucket, unquote_plus(dir_name))
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
            file_url = generate_absolute_url(bucket, unquote_plus(file_key))
            file_date = datetime.fromtimestamp(f["LastModified"].timestamp()).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            file_size = sizeof_fmt(f["Size"])
            entries.append(
                {
                    "name": os.path.basename(file_key),
                    "url": file_url,
                    "type": "file",
                    "date": file_date,
                    "size": file_size,
                }
            )

    # Render template
    current_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return template.render(
        entries=entries, directory=current_prefix, timestamp=current_timestamp
    )


def upload_index_html(client, bucket, prefix, html_content):
    try:
        client.put_object(
            Body=html_content,
            Bucket=bucket,
            Key=os.path.join(prefix, "index.html"),
            ContentType="text/html",
        )
        logging.info(f"Successfully uploaded index.html to {bucket}/{prefix}")
    except Exception as e:
        logging.error(f"Error uploading index.html to {bucket}/{prefix}: {e}")


def process_directory(bucket, prefix):
    client = boto3.client("s3")
    dirs, files = list_files(client, bucket, prefix)
     # Filter out the 'index.html' file if present
    files_without_index = [file for file in files if not file['Key'].endswith('index.html')]

    if not dirs and not files_without_index:
        # The directory is empty (or only contains 'index.html'), so delete 'index.html' if it exists
        try:
            client.delete_object(Bucket=bucket, Key=os.path.join(prefix, 'index.html'))
            logging.info(f"Removed 'index.html' from empty directory: {prefix}")
        except client.exceptions.NoSuchKey:
            # 'index.html' does not exist, no action needed
            pass
    else:
        # The directory is not empty, generate/update 'index.html'
        html_content = generate_index_html(bucket, files, dirs, prefix)
        upload_index_html(client, bucket, prefix, html_content)
    return [d["Prefix"] for d in dirs]


def main(s3_path):
    parsed_url = urlparse(s3_path)
    bucket = parsed_url.netloc
    prefix = parsed_url.path.lstrip("/")
    if not prefix.endswith("/"):
        prefix += "/"

    with ThreadPoolExecutor() as executor:
        futures = {executor.submit(process_directory, bucket, prefix)}
        while futures:
            done, futures = futures, set()
            for future in done:
                new_prefixes = future.result()
                for new_prefix in new_prefixes:
                    futures.add(executor.submit(process_directory, bucket, new_prefix))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        logging.error("Usage: python index.py s3://bucket-name/path")
        sys.exit(1)
    s3_path = sys.argv[1]
    main(s3_path)
