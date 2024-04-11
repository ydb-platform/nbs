LIBRARY()

SRCS(
    test_runtime.cpp
)

PEERDIR(
    contrib/ydb/apps/version
    contrib/ydb/library/actors/testlib
    library/cpp/testing/unittest
    contrib/ydb/core/base
    contrib/ydb/core/mon
    contrib/ydb/core/mon_alloc
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet
)

IF (GCC)
    CFLAGS(
        -fno-devirtualize-speculatively
    )
ENDIF()

END()

RECURSE_FOR_TESTS(
    ut
)
