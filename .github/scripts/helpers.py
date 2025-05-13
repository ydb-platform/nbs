import os
import math
import logging
import argparse

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


def setup_logger():
    formatter = MaskingFormatter("%(asctime)s: %(levelname)s: %(message)s")
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(console_handler)
    return logger


def github_output(key: str, value: str, is_secret: bool = False):
    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        with open(output_path, "a") as fp:
            fp.write(f"{key}={value}\n")
    logging.info(
        'echo "%s=%s" >> $GITHUB_OUTPUT', key, "******" if is_secret else value
    )


def convert_size(size_bytes):
    if size_bytes == 0:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB")
    i = math.floor(math.log(size_bytes, 1024))
    p = math.pow(1024, i)
    s = round(size_bytes / p, 2)
    return f"{s} {size_name[i]}"
