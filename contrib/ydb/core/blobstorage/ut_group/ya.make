UNITTEST()

IF (NOT WITH_VALGRIND)
    SRCS(
        main.cpp
    )
ENDIF()

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

PEERDIR(
    contrib/ydb/apps/version
    contrib/ydb/library/actors/interconnect/mock
    library/cpp/testing/unittest
    contrib/ydb/core/blobstorage/crypto
    contrib/ydb/core/blobstorage/dsproxy
    contrib/ydb/core/blobstorage/groupinfo
    contrib/ydb/core/blobstorage/pdisk/mock
    contrib/ydb/core/blobstorage/vdisk
    contrib/ydb/core/blobstorage/vdisk/common
    contrib/ydb/core/tx/scheme_board
    contrib/ydb/core/util
    contrib/ydb/core/util/actorsys_test
)

END()
