UNITTEST()

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

IF (BUILD_TYPE == "RELEASE" OR BUILD_TYPE == "RELWITHDEBINFO")
    SRCS(
        huge_cluster.cpp
    )
ELSE ()
    MESSAGE(WARNING "It takes too much time to run test in DEBUG mode, some tests are skipped")
ENDIF ()

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/interconnect
    contrib/ydb/library/actors/interconnect/ut/lib
    contrib/ydb/library/actors/interconnect/ut/protos
    library/cpp/testing/unittest
    contrib/ydb/library/actors/testlib
)

REQUIREMENTS(
    cpu:4
    ram:32
)

END()
