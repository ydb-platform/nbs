from __future__ import annotations

from pathlib import Path
from xml.etree import ElementTree as ET

from scripts.tests import generate_summary as gs
from scripts.tests import lint_report as lr


def _statuses(path: Path) -> list[gs.TestStatus]:
    root = ET.parse(path).getroot()
    return [
        gs.TestResult.from_junit(case).status for case in root.findall(".//testcase")
    ]


def test_lint_report_parses_flake8_violations(tmp_path: Path) -> None:
    log = tmp_path / "flake8.log"
    log.write_text(
        ".github/scripts/foo.py:7:13: F401 imported but unused\n"
        ".github/scripts/bar.py:9:1: SIM102 use a single if-statement\n"
    )
    junit = tmp_path / "flake8.xml"
    html = tmp_path / "flake8.html"

    cases = lr.build_cases(
        tool="flake8",
        title="GA Scripts flake8",
        command="flake8 .github/",
        exit_code=1,
        elapsed=1.0,
        log_text=log.read_text(),
        log_url="https://logs/flake8.log",
    )
    lr.write_junit(junit, cases)
    lr.write_html(html, cases, "https://summary/")

    root = ET.parse(junit).getroot()
    case = root.find(".//testcase")
    assert case is not None
    assert case.get("classname") == ".github/scripts/foo.py"
    assert case.get("name") == "7:13 F401"
    assert _statuses(junit) == [gs.TestStatus.FAIL, gs.TestStatus.FAIL]
    assert "https://logs/flake8.log" in html.read_text()


def test_lint_report_parses_black_reformat_lines(tmp_path: Path) -> None:
    cases = lr.build_cases(
        tool="black",
        title="GA Scripts black",
        command="black --check .github/",
        exit_code=1,
        elapsed=1.0,
        log_text="would reformat .github/scripts/foo.py\n",
        log_url="",
    )
    junit = tmp_path / "black.xml"
    lr.write_junit(junit, cases)

    root = ET.parse(junit).getroot()
    case = root.find(".//testcase")
    assert case is not None
    assert case.get("classname") == ".github/scripts"
    assert case.get("name") == "foo.py"
    assert _statuses(junit) == [gs.TestStatus.FAIL]


def test_lint_report_adds_single_pass_case_for_success(tmp_path: Path) -> None:
    cases = lr.build_cases(
        tool="flake8",
        title="GA Scripts flake8",
        command="flake8 .github/",
        exit_code=0,
        elapsed=1.0,
        log_text="",
        log_url="",
    )
    junit = tmp_path / "ok.xml"
    lr.write_junit(junit, cases)

    assert _statuses(junit) == [gs.TestStatus.PASS]


def test_lint_report_parses_shellcheck_gcc_lines(tmp_path: Path) -> None:
    cases = lr.build_cases(
        tool="shellcheck",
        title="GA Scripts shellcheck",
        command="shellcheck",
        exit_code=1,
        elapsed=1.0,
        log_text=".github/scripts/foo.sh:3:7: warning: Double quote to prevent globbing. [SC2086]\n",
        log_url="",
    )
    junit = tmp_path / "shellcheck.xml"
    lr.write_junit(junit, cases)

    root = ET.parse(junit).getroot()
    case = root.find(".//testcase")
    assert case is not None
    assert case.get("classname") == ".github/scripts/foo.sh"
    assert case.get("name") == "3:7 SC2086"
    assert _statuses(junit) == [gs.TestStatus.FAIL]


def test_lint_report_parses_shfmt_diff_blocks(tmp_path: Path) -> None:
    cases = lr.build_cases(
        tool="shfmt",
        title="GA Scripts shfmt",
        command="shfmt",
        exit_code=1,
        elapsed=1.0,
        log_text=(
            "--- .github/scripts/foo.sh.orig\n"
            "+++ .github/scripts/foo.sh\n"
            "@@ -1 +1 @@\n"
            "-if true; then echo ok; fi\n"
            "+if true; then\n"
            "+    echo ok\n"
            "+fi\n"
        ),
        log_url="",
    )
    junit = tmp_path / "shfmt.xml"
    lr.write_junit(junit, cases)

    root = ET.parse(junit).getroot()
    case = root.find(".//testcase")
    assert case is not None
    assert case.get("classname") == ".github/scripts"
    assert case.get("name") == "foo.sh:1"
    assert _statuses(junit) == [gs.TestStatus.FAIL]


def test_lint_report_parses_shfmt_file_list(tmp_path: Path) -> None:
    cases = lr.build_cases(
        tool="shfmt",
        title="GA Scripts shfmt",
        command="shfmt",
        exit_code=1,
        elapsed=1.0,
        log_text=".github/scripts/foo.sh\n.temporary/workflows/pr-1.sh\n",
        log_url="",
    )
    junit = tmp_path / "shfmt-list.xml"
    lr.write_junit(junit, cases)

    root = ET.parse(junit).getroot()
    parsed_cases = root.findall(".//testcase")
    assert parsed_cases[0].get("classname") == ".github/scripts"
    assert parsed_cases[0].get("name") == "foo.sh"
    assert parsed_cases[1].get("classname") == ".temporary/workflows"
    assert parsed_cases[1].get("name") == "pr-1.sh"
    assert _statuses(junit) == [gs.TestStatus.FAIL, gs.TestStatus.FAIL]


def test_lint_report_fallback_uses_title_for_missing_tools(tmp_path: Path) -> None:
    cases = lr.build_cases(
        tool="shellcheck",
        title="GA Scripts shellcheck (.github)",
        command="find .github -type f -print0 | xargs -0 shellcheck",
        exit_code=127,
        elapsed=1.0,
        log_text="xargs: shellcheck: No such file or directory\n",
        log_url="",
    )
    junit = tmp_path / "missing-tool.xml"
    lr.write_junit(junit, cases)

    root = ET.parse(junit).getroot()
    case = root.find(".//testcase")
    assert case is not None
    assert case.get("classname") == "shellcheck"
    assert case.get("name") == "GA Scripts shellcheck (.github)"
    assert "xargs: shellcheck: No such file or directory" in (
        case.find("failure").text or ""
    )
