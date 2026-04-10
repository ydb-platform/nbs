from __future__ import annotations

import json
from pathlib import Path
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


def _get_property_value(case: ET.Element, name: str) -> str | None:
    props = case.find("properties")
    if props is None:
        return None
    for prop in props.findall("property"):
        if prop.get("name") == name:
            return prop.get("value")
    return None


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
