from __future__ import annotations

from pathlib import Path
from typing import Callable
import xml.etree.ElementTree as ET

import pytest

TestCaseFactory = Callable[..., ET.Element]
WriteJunitXml = Callable[[Path, ET.Element], None]
BuildMultiSuiteXml = Callable[[Path, tuple[str, ...] | list[str]], None]


@pytest.fixture
def mk_testcase() -> TestCaseFactory:
    def _mk(
        *,
        classname: str = "suite.case",
        name: str = "test",
        time: str = "1.0",
        failure: str | None = None,
        error: str | None = None,
        skipped: bool = False,
        props: dict[str, str] | None = None,
    ) -> ET.Element:
        testcase = ET.Element(
            "testcase",
            {"classname": classname, "name": name, "time": time},
        )

        if failure is not None:
            node = ET.SubElement(testcase, "failure")
            node.text = failure

        if error is not None:
            node = ET.SubElement(testcase, "error")
            node.text = error

        if skipped:
            ET.SubElement(testcase, "skipped")

        if props:
            props_node = ET.SubElement(testcase, "properties")
            for key, value in props.items():
                ET.SubElement(props_node, "property", {"name": key, "value": value})

        return testcase

    return _mk


@pytest.fixture
def write_junit_xml() -> WriteJunitXml:
    def _write(path: Path, *cases: ET.Element, suite_name: str = "test-suite") -> None:
        suite = ET.Element("testsuite", {"name": suite_name})
        for case in cases:
            suite.append(case)

        root = ET.Element("testsuites")
        root.append(suite)
        ET.ElementTree(root).write(path)

    return _write


@pytest.fixture
def build_multi_suite_xml() -> BuildMultiSuiteXml:
    def _build(path: Path, suite_names: tuple[str, ...] | list[str]) -> None:
        root = ET.Element("testsuites")
        for suite_name in suite_names:
            suite = ET.SubElement(root, "testsuite", {"name": suite_name})
            ET.SubElement(suite, "testcase", {"classname": suite_name, "name": "t"})
        ET.ElementTree(root).write(path)

    return _build
