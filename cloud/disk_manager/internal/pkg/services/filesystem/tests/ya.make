OWNER(g:cloud-nbs)

GO_TEST_FOR(cloud/disk_manager/internal/pkg/services/filesystem)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/service-kikimr.inc)

GO_XTEST_SRCS(
    filesystem_test.go
)

SIZE(MEDIUM)

END()
