from __future__ import annotations

from pathlib import Path

from scripts.tests import pytest_summary as ps


def test_pytest_summary_main_writes_plain_count_table(
    tmp_path: Path, monkeypatch, mk_testcase, write_junit_xml, capsys
) -> None:
    xml_path = tmp_path / "junit.xml"
    write_junit_xml(
        xml_path,
        mk_testcase(classname="a", name="pass"),
        mk_testcase(classname="a", name="fail", failure="boom"),
    )

    monkeypatch.setattr(
        "sys.argv",
        ["pytest_summary.py", "--title", "GA Scripts pytest", str(xml_path)],
    )

    ps.main()

    out = capsys.readouterr().out
    assert "| TESTS | PASSED | ERRORS | FAILED | FAILED BUILD | SKIPPED | MUTED" in out
    assert "| 2 | 1 | 0 | 1 | 0 | 0 | 0 |" in out
