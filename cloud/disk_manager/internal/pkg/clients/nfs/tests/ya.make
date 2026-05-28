GO_TEST_FOR(cloud/disk_manager/internal/pkg/clients/nfs)

SET_APPEND(RECIPE_ARGS --nfs-only)
SET_APPEND(RECIPE_ARGS --allow-filestore-force-destroy)

INCLUDE(${ARCADIA_ROOT}/cloud/disk_manager/test/recipe/recipe.inc)

GO_XTEST_SRCS(
    client_metrics_test.go
    client_test.go
    session_with_re_establish_test.go
)

SIZE(MEDIUM)

END()
