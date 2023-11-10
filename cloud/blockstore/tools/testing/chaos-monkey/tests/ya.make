GO_TEST_FOR(cloud/blockstore/tools/testing/chaos-monkey)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

DEPENDS(
    cloud/blockstore/tools/testing/chaos-monkey
)

END()
