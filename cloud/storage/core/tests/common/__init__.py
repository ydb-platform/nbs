import logging
import os
import re

import yatest.common as common

yatest_logger = logging.getLogger("ya.test")


def append_recipe_err_files(common_file_name: str, err_file_path: str) -> None:
    """
    Append a file path to the common_file_name.

    If the variable is not set, it initializes it with the given file path.
    """
    yatest_logger.debug("Appending recipe error file: %s", err_file_path)

    if not os.path.exists(common_file_name):
        with open(common_file_name, "w") as f:
            yatest_logger.debug("Created new common file: %s", common_file_name)
            pass

    with open(common_file_name, "a") as f:
        f.write(err_file_path + "\n")
        yatest_logger.debug("Appended: %s", err_file_path)


def process_recipe_err_files(common_file_name: str) -> list[str]:
    """
    Process the error files listed in the RECIPE_ERR_FILES environment variable.

    Checks each file for sanitizer errors and logs the results.
    """
    errors = []
    if not os.path.exists(common_file_name):
        yatest_logger.debug("No common file found: %s", common_file_name)
        return errors

    with open(common_file_name, "r") as f:
        err_files = f.readlines()
    err_files = [file.strip() for file in err_files if file.strip()]
    yatest_logger.debug("Processing recipe error files: %s", err_files)

    for file_path in err_files:
        file_path = file_path.strip()
        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                std_err = f.read()
            match = re.search(common.SANITIZER_ERROR_PATTERN, std_err)
            if match:
                truncated_std_err = common.process.truncate(
                    str(std_err).split("Sanitizer")[1], common.process.MAX_OUT_LEN
                )
                sanitizer_name = str(match.group(1)).strip()
                yatest_logger.error(
                    "%s sanitizer found errors:\n\tstd_err:%s\n",
                    sanitizer_name,
                    truncated_std_err,
                )
                errors.append(
                    f"{sanitizer_name} sanitizer found errors in {file_path}:\n{truncated_std_err}"
                )
            else:
                yatest_logger.debug("No sanitizer errors found")
    return errors
