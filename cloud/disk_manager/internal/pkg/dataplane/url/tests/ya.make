GO_TEST_FOR(cloud/disk_manager/internal/pkg/dataplane/url)

INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/images/recipe/recipe.inc)

GO_XTEST_SRCS(
    url_test.go
)

IF (RACE)
    SIZE(LARGE)
    TAG(ya:fat ya:force_sandbox ya:sandbox_coverage)
ELSE()
    SIZE(MEDIUM)
ENDIF()

TAG(sb:ssd)

REQUIREMENTS(
    ram:32
)

END()
