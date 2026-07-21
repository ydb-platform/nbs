PY3TEST()

SIZE(MEDIUM)
TIMEOUT(240)

TEST_SRCS(
    test.py
)

DEPENDS(
    contrib/ydb/library/yql/tools/purebench
)

END()
