OWNER(g:cloud-nbs)

GO_TEST_FOR(cloud/disk_manager/internal/pkg/logging)

GO_XTEST_SRCS(
    journald_logger_test.go
)

SIZE(LARGE)
TAG(ya:fat ya:privileged ya:force_sandbox ya:sandbox_coverage)

REQUIREMENTS(
    cpu:4
    ram:16
    container:1268336763
)

END()
