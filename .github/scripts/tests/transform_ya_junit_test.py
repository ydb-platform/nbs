from __future__ import annotations

import json
from pathlib import Path
from textwrap import dedent
import xml.etree.ElementTree as ET

import pytest

from scripts.tests import transform_ya_junit as tyj


def _write_junit(path: Path) -> None:
    root = ET.Element("testsuites")
    suite = ET.SubElement(root, "testsuite", {"name": "suite-name"})
    case = ET.SubElement(
        suite,
        "testcase",
        {"classname": "old.class", "name": "pkg.TestClass.test_method", "time": "1.0"},
    )
    failure = ET.SubElement(case, "failure")
    failure.text = "boom"
    ET.ElementTree(root).write(path)


def _write_trace(ya_out: Path) -> None:
    trace_dir = ya_out / "suite-name" / "test-results" / "run-1"
    trace_dir.mkdir(parents=True)
    trace = trace_dir / "ytest.report.trace"
    trace.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "name": "subtest-finished",
                        "value": {
                            "class": "pkg::TestClass",
                            "subtest": "test_method",
                            "logs": {
                                "stdout": "$(BUILD_ROOT)/suite-name/stdout.log",
                                "logsdir": "$(BUILD_ROOT)/suite-name/logs",
                            },
                        },
                    }
                )
            ]
        )
    )


def _write_chunk_trace(ya_out: Path, suite_name: str, runner: str) -> None:
    trace_dir = ya_out / suite_name / "test-results" / runner
    trace_dir.mkdir(parents=True)
    trace = trace_dir / "ytest.report.trace"
    trace.write_text(
        json.dumps(
            {
                "name": "chunk-event",
                "value": {
                    "chunk_index": 0,
                    "nchunks": 1,
                    "logs": {
                        "log": f"$(BUILD_ROOT)/{suite_name}/test-results/{runner}/run_test.log",
                        "logsdir": f"$(BUILD_ROOT)/{suite_name}/test-results/{runner}/testing_out_stuff",
                    },
                },
            }
        )
    )


def _get_property_value(case: ET.Element, name: str) -> str | None:
    props = case.find("properties")
    if props is None:
        return None
    for prop in props.findall("property"):
        if prop.get("name") == name:
            return prop.get("value")
    return None


def _write_issue_junit(
    report: Path, suite_name: str, *, include_py3test_paths: bool
) -> None:
    log_paths = (
        f"""
            log:
            /home/github/tmp_test/out/{suite_name}/test-results/py3test/run_test.log
            logsdir:
            /home/github/tmp_test/out/{suite_name}/test-results/py3test/testing_out_stuff
            recipe stderr:
            /home/github/tmp_test/out/{suite_name}/test-results/py3test/testing_out_stuff/recipe_stop_vhost-endpoint-recipe-3.err
            recipe stdout:
            /home/github/tmp_test/out/{suite_name}/test-results/py3test/testing_out_stuff/recipe_stop_vhost-endpoint-recipe-3.out
            stderr:
            /home/github/tmp_test/out/{suite_name}/test-results/py3test/testing_out_stuff/stderr"""
        if include_py3test_paths
        else ""
    )
    stderr_tail = (
        "Stderr tail:bf7e&quot;\\n"
        "2026-06-29T16:03:20.195137Z :NFS_CLIENT TRACE: "
        "cloud/filestore/libs/client/client.cpp:341: StopEndpoint "
        "#12786235345919440230 response received: Error {{ Code: 2147614724 "
        "Message: &quot;Deadline Exceeded&quot; }}\\n"
        "2026-06-29T16:03:20.195191Z :NFS_CLIENT TRACE: "
        "cloud/filestore/libs/client/client.cpp:367: StopEndpoint "
        "#12786235345919440230 request completed\\n"
        "filestore-client &quot;/home/github/tmp_test/out/cloud/filestore/apps/"
        "client/filestore-client stopendpoint --socket-path "
        "/tmp/test.vhost.eba2e719-d7ee-46ad-a48c-f6567c08bf7e "
        "--server-address localhost --server-port 28502 --verbose trace&quot; "
        "failed: (NCloud::TServiceError) "
        "cloud/filestore/apps/client/lib/stop_endpoint.cpp:37: "
        "E_RETRY_TIMEOUT Retry timeout (2147483662). Deadline Exceeded | \\n"
        "2026-06-29T16:03:20.195739Z :NFS_CLIENT INFO: "
        "cloud/filestore/libs/client/client.cpp:656: Shutting down\\n"
        "2026-06-29T16:03:20.195750Z :NFS_CLIENT TRACE: "
        "cloud/storage/core/libs/grpc/executor.h:113: "
        "CLI1 executor's shutdown() started\\n'"
    )

    report.write_text(dedent(f"""\
            <?xml version="1.0" ?>
            <testsuites>
                <testsuite name="{suite_name}" tests="3" time="204.62756158700086" failures="1" skipped="0">
                    <testcase name="sole chunk chunk" time="0.0">
                        <failure>RecipeTearDownError: vhost-endpoint-recipe-3 failed
            {stderr_tail}
            {log_paths}</failure>
                    </testcase>
                    <testcase name="test.py.test_create_unlink_steal" time="88.28822920200037"/>
                    <testcase name="test.py.test_create_list" time="34.255492412999956"/>
                    <testcase name="test.py.test_create_unlink_steal_list_nodes_internal" time="82.08383997200053"/>
                </testsuite>
            </testsuites>
            """))


def test_transform_adds_links_and_copies_logs(tmp_path: Path) -> None:
    ya_out = tmp_path / "ya-out"
    logs_file = ya_out / "suite-name" / "stdout.log"
    logs_file.parent.mkdir(parents=True)
    logs_file.write_text("hello-log")

    _write_trace(ya_out)

    report = tmp_path / "junit.xml"
    _write_junit(report)

    output = tmp_path / "out.xml"
    logs_out = tmp_path / "logs-out"

    with report.open("r") as fp:
        tyj.transform(
            fp,
            tyj.YaMuteCheck(),
            str(ya_out),
            False,
            "https://logs/",
            str(logs_out),
            0,
            str(output),
            "https://data",
        )

    tree = ET.parse(output)
    case = tree.getroot().find("./testsuite/testcase")

    assert case.get("classname") == "suite-name"
    assert (
        _get_property_value(case, "url:stdout") == "https://logs/suite-name/stdout.log"
    )
    assert (
        _get_property_value(case, "url:logs_directory")
        == "https://data/suite-name/logs"  # noqa: W503
    )
    assert (logs_out / "suite-name" / "stdout.log").exists()


def test_save_log_applies_truncation(tmp_path: Path) -> None:
    src_dir = tmp_path / "src"
    src_dir.mkdir()
    src = src_dir / "a.log"
    src.write_bytes(b"1234567890")

    out_dir = tmp_path / "out"

    url = tyj.save_log(str(src_dir), str(src), str(out_dir), "https://logs/", 4)

    assert url == "https://logs/a.log"
    assert (out_dir / "a.log").read_bytes() == b"7890"


def test_transform_skips_malformed_chunk_name_without_crash(tmp_path: Path) -> None:
    root = ET.Element("testsuites")
    suite = ET.SubElement(root, "testsuite", {"name": "suite-name"})
    case = ET.SubElement(
        suite,
        "testcase",
        {"classname": "old.class", "name": "chunk bad format", "time": "1.0"},
    )
    failure = ET.SubElement(case, "failure")
    failure.text = "boom"
    report = tmp_path / "junit.xml"
    ET.ElementTree(root).write(report)

    output = tmp_path / "out.xml"
    with report.open("r") as fp:
        tyj.transform(
            fp,
            tyj.YaMuteCheck(),
            str(tmp_path / "ya-out"),
            False,
            "https://logs/",
            str(tmp_path / "logs-out"),
            0,
            str(output),
            "https://data",
        )

    tree = ET.parse(output)
    parsed_case = tree.getroot().find("./testsuite/testcase")
    assert parsed_case is not None
    assert parsed_case.find("properties") is None


def test_transform_uses_real_issue_xml_to_pick_py3test_chunk_trace(
    tmp_path: Path,
) -> None:
    suite_name = "cloud/filestore/tests/fmdtest/qemu-kikimr-nemesis-test"
    ya_out = tmp_path / "ya-out"

    for runner in ("py3test", "flake8"):
        _write_chunk_trace(ya_out, suite_name, runner)
        log_file = ya_out / suite_name / "test-results" / runner / "run_test.log"
        log_file.write_text(f"{runner}-log")

    report = tmp_path / "junit.xml"
    _write_issue_junit(report, suite_name, include_py3test_paths=True)

    output = tmp_path / "out.xml"
    logs_out = tmp_path / "logs-out"
    with report.open("r") as fp:
        tyj.transform(
            fp,
            tyj.YaMuteCheck(),
            str(ya_out),
            False,
            "https://logs/",
            str(logs_out),
            0,
            str(output),
            "https://data",
        )

    parsed_case = ET.parse(output).getroot().find("./testsuite/testcase")
    assert parsed_case is not None
    assert (
        _get_property_value(parsed_case, "url:logs_directory")
        == f"https://data/{suite_name}/test-results/py3test/testing_out_stuff"
    )
    assert (
        _get_property_value(parsed_case, "url:log")
        == f"https://logs/{suite_name}/test-results/py3test/run_test.log"
    )
    assert (
        logs_out / suite_name / "test-results" / "py3test" / "run_test.log"
    ).exists()


def test_transform_does_not_guess_ambiguous_chunk_trace(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    suite_name = "cloud/filestore/tests/fmdtest/qemu-kikimr-nemesis-test"
    ya_out = tmp_path / "ya-out"

    for runner in ("py3test", "flake8"):
        _write_chunk_trace(ya_out, suite_name, runner)
        log_file = ya_out / suite_name / "test-results" / runner / "run_test.log"
        log_file.write_text(f"{runner}-log")

    report = tmp_path / "junit.xml"
    _write_issue_junit(report, suite_name, include_py3test_paths=False)

    output = tmp_path / "out.xml"
    github_summary = tmp_path / "github_step_summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(github_summary))

    with report.open("r") as fp:
        tyj.transform(
            fp,
            tyj.YaMuteCheck(),
            str(ya_out),
            False,
            "https://logs/",
            str(tmp_path / "logs-out"),
            0,
            str(output),
            "https://data",
        )

    parsed_case = ET.parse(output).getroot().find("./testsuite/testcase")
    assert parsed_case is not None
    assert parsed_case.find("properties") is None
    assert (
        f"**JUnit transform warning:** Unable to disambiguate chunk logs for {suite_name} [0/1]; leaving log links empty"
        in github_summary.read_text()
    )


@pytest.mark.parametrize(
    "suite_name,test_name,expected",
    [
        (
            "cloud/filestore/tests/fio_index/mount-kikimr-test",
            "any.test.name",
            True,
        ),
        (
            "cloud/storage/core/libs/kikimr/ut",
            "TConfigInitializerTest.ShouldAdjustActorSystemThreadsAccordingToAvailableCpuCores",
            True,
        ),
        (
            "cloud/storage/core/libs/kikimr/ut",
            "TConfigInitializerTest.OtherTest",
            False,
        ),
    ],
)
def test_ya_mute_check_loads_real_style_rules(
    tmp_path: Path, suite_name: str, test_name: str, expected: bool
) -> None:
    mute_file = tmp_path / "muted_ya.txt"
    mute_file.write_text(
        "\n".join(
            [
                "cloud/filestore/tests/fio_index/mount-kikimr-test *",
                "cloud/storage/core/libs/kikimr/ut TConfigInitializerTest.ShouldAdjustActorSystemThreadsAccordingToAvailableCpuCores",  # noqa: E501
            ]
        )
    )

    check = tyj.YaMuteCheck()
    check.load(str(mute_file))

    case = ET.Element("testcase", {"classname": suite_name, "name": test_name})
    assert check(suite_name, test_name, case) is expected


def test_ya_mute_check_ignores_invalid_config_lines(tmp_path: Path) -> None:
    mute_file = tmp_path / "muted_ya.txt"
    mute_file.write_text(
        "\n".join(
            [
                "invalid-line-without-test-name",
                "cloud/filestore/tests/fio_index/mount-local-test *",
            ]
        )
    )

    check = tyj.YaMuteCheck()
    check.load(str(mute_file))

    # only one valid rule should be loaded
    assert len(check.regexps) == 1


def test_ya_mute_check_chunk_mode_requires_all_failed_subtests_match() -> None:
    check = tyj.YaMuteCheck()
    check.populate("cloud/storage/core/libs/kikimr/ut", "TConfigInitializerTest.*")

    suite_name = "cloud/storage/core/libs/kikimr/ut"
    test_name = "chunk [1/2]"
    case = ET.Element("testcase", {"classname": suite_name, "name": test_name})
    failure = ET.SubElement(case, "failure")
    failure.text = (
        "List of the tests involved in the launch:\n"
        "  TConfigInitializerTest::ShouldAdjustActorSystemThreadsAccordingToAvailableCpuCores duration: 0.1\n"
        "  TConfigInitializerTest::AnotherCase duration: 0.1\n"
    )

    assert check(suite_name, test_name, case) is True

    failure.text += "  OtherClass::NotMuted duration: 0.1\n"
    assert check(suite_name, test_name, case) is False
