PY2TEST()

TAG(ya:manual)

TEST_SRCS(
    test.py
)

IF (SANITIZER_TYPE)
    TIMEOUT(1800)
    SIZE(LARGE)
    TAG(ya:fat sb:ttl=2)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
    TAG(sb:ttl=2)
ENDIF()

FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

DEPENDS(
    contrib/ydb/library/yql/tools/yqlrun
    contrib/ydb/library/yql/tools/astdiff
    contrib/ydb/library/yql/tests/common/test_framework/udfs_deps
    contrib/ydb/library/yql/udfs/test/test_import
    contrib/ydb/library/yql/udfs/test/simple
)

DATA(
    arcadia/contrib/ydb/library/yql/tests/s-expressions # python files
    arcadia/contrib/ydb/library/yql/mount
    arcadia/contrib/ydb/library/yql/cfg/tests
)

PEERDIR(
    library/python/testing/swag/lib
    contrib/ydb/library/yql/protos
    contrib/ydb/library/yql/tests/common/test_framework
)

TAG(ya:dump_test_env)

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

NO_CHECK_IMPORTS()

REQUIREMENTS(cpu:4 ram:13)

END()

