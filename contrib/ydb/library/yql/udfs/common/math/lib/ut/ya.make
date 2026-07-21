IF (OS_LINUX)
IF (NOT WITH_VALGRIND)
    UNITTEST_FOR(contrib/ydb/library/yql/udfs/common/math/lib)

    SRCS(
        round_ut.cpp
    )

    END()
ENDIF()
ENDIF()
