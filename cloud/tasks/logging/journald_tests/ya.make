GO_TEST_FOR(cloud/tasks/logging)

GO_XTEST_SRCS(
    journald_logger_test.go
)

SIZE(LARGE)
TAG(
    ya:fat
    ya:privileged
    ya:force_sandbox
    ya:sandbox_coverage
    ya:large_tests_on_single_slots
)

REQUIREMENTS(
    cpu:4
    ram:16
    container:1268336763
)

END()
