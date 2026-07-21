IF (OS_LINUX)
    IF (NOT WITH_VALGRIND)
        UNITTEST_FOR(contrib/ydb/library/yql/udfs/common/python/bindings)

        INCLUDE(../ya.make.test.inc)
        PEERDIR(
            library/python/type_info
        )
        USE_PYTHON3()
        END()
    ENDIF()
ENDIF()
