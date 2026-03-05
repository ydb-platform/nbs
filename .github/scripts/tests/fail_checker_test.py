import pytest

from scripts.tests import fail_checker as fc


def test_check_for_fail_returns_237_for_build_failures(
    tmp_path, monkeypatch, mk_testcase, write_junit_xml
):
    report = tmp_path / "junit.xml"
    write_junit_xml(
        report,
        mk_testcase(classname="a", name="b", failure="skipped due to a failed build"),
    )

    env_file = tmp_path / "env"
    monkeypatch.setenv("FAIL_CHECKER_TEMP_FILE", str(env_file))

    with pytest.raises(SystemExit) as err:
        fc.check_for_fail([str(report)])

    assert err.value.code == 237
    assert "BUILD_FAILED_COUNT=1" in env_file.read_text()


def test_check_for_fail_returns_1_for_regular_failures(
    tmp_path, mk_testcase, write_junit_xml
):
    report = tmp_path / "junit.xml"
    write_junit_xml(report, mk_testcase(classname="a", name="b", failure="boom"))

    with pytest.raises(SystemExit) as err:
        fc.check_for_fail([str(report)])

    assert err.value.code == 1


def test_get_fail_dirs_prints_unique_classnames(
    tmp_path, capsys, mk_testcase, write_junit_xml
):
    report = tmp_path / "junit.xml"
    write_junit_xml(
        report,
        mk_testcase(classname="a", name="b", failure="boom"),
        mk_testcase(classname="a", name="c", error="err"),
        mk_testcase(classname="z", name="d", error="err"),
    )

    fc.get_fail_dirs([str(report)])

    out = capsys.readouterr().out.strip().splitlines()
    assert out == ["a", "z"]
