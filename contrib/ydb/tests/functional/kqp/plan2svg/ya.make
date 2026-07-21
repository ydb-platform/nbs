PY3TEST()

TEST_SRCS(
    test_cte.py
)

SIZE(MEDIUM)

DEPENDS(
    contrib/ydb/tests/tools/kqprun
)

DATA(
    arcadia/contrib/ydb/tests/tools/kqprun/configuration/app_config.conf
)

END()
