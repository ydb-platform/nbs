import io

from scripts.tests import log_parser as lp


def test_parse_gtest_fails_extracts_failure_body():
    log = [
        "[ RUN      ] A.B\n",
        "line1\n",
        "[  FAILED  ] A.B (1 ms)\n",
    ]

    items = list(lp.parse_gtest_fails(log))
    assert items == [("A", "B", ["line1\n"])]


def test_parse_yunit_fails_extracts_failed_exec_block():
    log = [
        "[exec] A::B...\n",
        "noise\n",
        "[FAIL] reason\n",
        "-----> done\n",
    ]

    items = list(lp.parse_yunit_fails(log))
    assert len(items) == 1
    assert items[0][0] == "A"
    assert items[0][1] == "B...\n"


def test_ctest_log_parser_parses_target_reason_and_output():
    text = io.StringIO(
        " 1/2 Test #1: unit.test ....***Failed  0.01 sec\n"
        "stack line\n"
        "100% tests passed\n"
    )

    items = list(lp.ctest_log_parser(text))
    assert items == [("unit.test", "Failed", ["stack line"])]
