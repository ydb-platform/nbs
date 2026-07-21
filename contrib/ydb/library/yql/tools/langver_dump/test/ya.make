PY3TEST()

TEST_SRCS(
    test.py
)

DEPENDS(
    contrib/ydb/library/yql/tools/langver_dump
)

DATA(
    arcadia/contrib/ydb/library/yql/data/language
)

END()
