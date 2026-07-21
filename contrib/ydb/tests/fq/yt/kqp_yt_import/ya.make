PY3TEST()

TEST_SRCS(
    test_ctas.py
    test_yt_reading.py
)

PY_SRCS(
    conftest.py
    helpers.py
)

SIZE(MEDIUM)
REQUIREMENTS(cpu:2)

DEPENDS(
    contrib/ydb/tests/tools/kqprun
)

DATA(
    arcadia/contrib/ydb/tests/fq/yt/cfg
    arcadia/contrib/ydb/tests/fq/yt/kqp_yt_import
)

PEERDIR(
    contrib/ydb/public/api/protos
    contrib/ydb/tests/fq/tools
    contrib/ydb/library/yql/tests/common/test_framework
)

END()
