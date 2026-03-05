#!/usr/bin/env python3

# Fake Flake8 Linter: just writes empty result file

import sys
import os
import json


def main():
    args = sys.argv
    for i, arg in enumerate(args):
        print(arg)
        # Check if the argument is an existing file
        if os.path.isfile(arg) and i > 0 and args[i - 1].startswith("--params"):
            # Sample contents:     {"source_root": "$(SOURCE_ROOT)", "project_path": "cloud/filestore/tests/profile_log/replay", "output_path": "/home/nbsci/nbs/cloud/filestore/tests/profile_log/replay/test-results/flake8/testing_out_stuff", "lint_name": "flake8", "files": ["cloud/filestore/tests/profile_log/replay/test.py", "cloud/filestore/tests/profile_log/replay/test_grpc.py"], "depends": {"cloud/filestore/tests/profile_log/replay": "/home/nbsci/nbs/cloud/filestore/tests/profile_log/replay", "tools/flake8_linter/flake8_linter": "/home/nbsci/nbs/tools/flake8_linter/flake8_linter"}, "global_resources": {"FLAKE8_PY3_RESOURCE_GLOBAL": "/home/nbsci/.ya/tools/v3/3968796477", "LLD_ROOT_RESOURCE_GLOBAL": "/home/nbsci/.ya/tools/v3/5458408674", "OS_SDK_ROOT_RESOURCE_GLOBAL": "/home/nbsci/.ya/tools/v3/309054781"}, "configs": ["build/config/tests/flake8/flake8.conf"], "extra_params": {"DISABLE_FLAKE8_MIGRATIONS": "yes"}, "report_file": "/home/nbsci/nbs/cloud/filestore/tests/profile_log/replay/test-results/flake8/testing_out_stuff/linter_report.json"}
            try:
                with open(arg, "r") as file:
                    contents = file.read()

                # Parse JSON and write {} to report_file
                params = json.loads(contents)
                report_file = params.get("report_file")
                if report_file:
                    # Ensure the directory exists
                    os.makedirs(os.path.dirname(report_file), exist_ok=True)
                    # Write empty JSON object to report file
                    with open(report_file, "w") as f:
                        f.write('{"report":{}}')
                    print(f"Written '{...}' to {report_file}")
                else:
                    print("No report_file found in params")
            except Exception as e:
                print(f"Error processing file {arg}: {e}")


if __name__ == "__main__":
    main()
