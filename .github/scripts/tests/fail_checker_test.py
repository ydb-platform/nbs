import xml.etree.ElementTree as ET

import pytest

from scripts.tests import fail_checker as fc


def _write_xml(path, *cases):
    suite = ET.Element("testsuite", {"name": "suite"})
    for case in cases:
        suite.append(case)

    root = ET.Element("testsuites")
    root.append(suite)
    ET.ElementTree(root).write(path)


def _case(classname, name, failure=None, error=None):
    node = ET.Element("testcase", {"classname": classname, "name": name})
    if failure is not None:
        failure_node = ET.SubElement(node, "failure")
        failure_node.text = failure
    if error is not None:
        error_node = ET.SubElement(node, "error")
        error_node.text = error
    return node


def test_check_for_fail_returns_237_for_build_failures(tmp_path, monkeypatch):
    report = tmp_path / "junit.xml"
    _write_xml(
        report,
        _case("a", "b", failure="skipped due to a failed build"),
    )

    env_file = tmp_path / "env"
    monkeypatch.setenv("FAIL_CHECKER_TEMP_FILE", str(env_file))

    with pytest.raises(SystemExit) as err:
        fc.check_for_fail([str(report)])

    assert err.value.code == 237
    assert "BUILD_FAILED_COUNT=1" in env_file.read_text()


def test_check_for_fail_returns_1_for_regular_failures(tmp_path):
    report = tmp_path / "junit.xml"
    _write_xml(report, _case("a", "b", failure="boom"))

    with pytest.raises(SystemExit) as err:
        fc.check_for_fail([str(report)])

    assert err.value.code == 1


def test_get_fail_dirs_prints_unique_classnames(tmp_path, capsys):
    report = tmp_path / "junit.xml"
    _write_xml(
        report,
        _case("a", "b", failure="boom"),
        _case("a", "c", error="err"),
        _case("z", "d", error="err"),
    )

    fc.get_fail_dirs([str(report)])

    out = set(capsys.readouterr().out.strip().splitlines())
    assert out == {"a", "z"}
