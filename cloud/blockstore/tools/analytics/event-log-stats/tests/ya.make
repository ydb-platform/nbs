PY3TEST()

SIZE(SMALL)

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
