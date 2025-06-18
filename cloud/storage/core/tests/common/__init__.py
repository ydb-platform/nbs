import logging
import os
import re

import yatest.common as common

yatest_logger = logging.getLogger("ya.test")


def append_recipe_err_files(common_file_name: str, err_file_path: str) -> None:
    """
    Append a file path to the RECIPE_ERR_FILES environment variable.

    If the variable is not set, it initializes it with the given file path.
    """
    yatest_logger.debug("XXX Appending recipe error file: %s", err_file_path)
    with open(common_file_name, "a") as f:
        f.write(err_file_path + "\n")
        yatest_logger.debug("XXX Appended: %s", err_file_path)


def process_recipe_err_files(common_file_name: str) -> list[str]:
    """
    Process the error files listed in the RECIPE_ERR_FILES environment variable.

    Checks each file for sanitizer errors and logs the results.
    """
    with open(common_file_name, "r") as f:
        err_files = f.readlines()
    err_files = [file.strip() for file in err_files if file.strip()]
    yatest_logger.debug("XXX Processing recipe error files: %s", err_files)
    errors = []
    for file_path in err_files:
        file_path = file_path.strip()
        if os.path.exists(file_path):
            with open(file_path, "rb") as f:
                std_err = f.read()
            match = re.search(common.SANITIZER_ERROR_PATTERN, std_err)
            if match:
                truncated_std_err = common.process.truncate(
                    std_err, common.process.MAX_OUT_LEN
                )
                yatest_logger.error(
                    "%s sanitizer found errors:\n\tstd_err:%s\n",
                    match.group(1),
                    truncated_std_err,
                )
                errors.append(
                    f"{match.group(1)} sanitizer found errors in {file_path}:\n{truncated_std_err}"
                )
            else:
                yatest_logger.debug("No sanitizer errors found")
    return errors
