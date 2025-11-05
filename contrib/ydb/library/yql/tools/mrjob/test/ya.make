IF(OS_LINUX)
    PY3TEST()

    TAG(ya:manual)

    TEST_SRCS(test.py)

    DEPENDS(
        contrib/ydb/library/yql/tools/mrjob
    )

    END()
ENDIF()
