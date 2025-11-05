PY2TEST()

TEST_SRCS(
    test.py
)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat sb:ttl=2)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
    TAG(sb:ttl=2)
ENDIF()

DEPENDS(
    contrib/ydb/library/yql/tests/common/test_framework/udfs_deps
    contrib/ydb/library/yql/udfs/test/test_import
    contrib/ydb/tests/tools/kqprun
)

DATA(
    arcadia/contrib/ydb/library/yql/tests/sql
    arcadia/contrib/ydb/tests/fq/yt
    arcadia/contrib/ydb/tests/fq/yt/cfg
)

PEERDIR(
    contrib/ydb/library/yql/tests/common/test_framework
)

NO_CHECK_IMPORTS()

REQUIREMENTS(ram:20)

END()
