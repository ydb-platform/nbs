PY3TEST()

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/small.inc)

TEST_SRCS(
    test.py
)

DEPENDS(
    cloud/blockstore/tools/analytics/event-log-stats
)

DATA(
    arcadia/cloud/blockstore/tools/analytics/event-log-stats/tests/data
)

END()
