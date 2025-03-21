import pytest

import yatest.common as common

tests = ["sqlite", "jpeg", "fstest"]


def _get_bindir():
    return common.build_path(
        "cloud/filestore/tests/profile_log/replay/data")


def run_replay(name):
    dir_out_path = common.output_path() + "/replay"
    conf_body = """
Tests {
    LoadTest {
        Name: "test"
        KeepFileStore: true
        ReplayFsSpec {
            FileName: \"""" + _get_bindir() + "/" + name + ".log" + """\"
            ReplayRoot: \"""" + dir_out_path + """\"
        }
        IODepth: 64
    }
}
"""

    tool_conf_path = common.output_path() + "/config.txt"
    with open(tool_conf_path, "w+") as f:
        f.writelines(conf_body)

    tool_bin_path = common.binary_path("cloud/filestore/tools/testing/loadtest/bin/filestore-loadtest")
    common.execute([tool_bin_path,  "--tests-config", tool_conf_path])

    proc = common.execute(["bash", "-xc", " cd " + dir_out_path + " && find . -type f -iname '*' -printf '%h/%f %s \n' | sort "])
    return proc.stdout.decode('utf-8')


@pytest.mark.parametrize("name", tests)
def test_profile_log(name):
    results_path = common.output_path("results.txt")
    result = run_replay(name)

    with open(results_path, 'w') as results:
        results.write(result)

    ret = common.canonical_file(results_path, local=True)
    return ret
