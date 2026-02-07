PY3TEST()

TAG(ya:manual)


    TEST_SRCS(
        test_sql2yql.py
        test_sql_negative.py
        test_sql_format.py
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
    SPLIT_FACTOR(5)
    DEPENDS(
        contrib/ydb/library/yql/tools/sql2yql
        contrib/ydb/library/yql/tools/yqlrun
        contrib/ydb/library/yql/tools/sql_formatter
        contrib/libs/protobuf/python
    )
    DATA(
        arcadia/contrib/ydb/library/yql/tests/sql # python files
        arcadia/contrib/ydb/library/yql/mount
        arcadia/contrib/ydb/library/yql/cfg/tests
    )
    PEERDIR(
        contrib/ydb/library/yql/tests/common/test_framework
        library/python/testing/swag/lib
    )


NO_CHECK_IMPORTS()

REQUIREMENTS(ram:12)

END()

