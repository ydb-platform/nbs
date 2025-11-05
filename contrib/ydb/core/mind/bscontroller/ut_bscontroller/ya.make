UNITTEST()

FORK_SUBTESTS()

REQUIREMENTS(
    cpu:4
    ram:16
)

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
    TIMEOUT(1800)
ELSE()
    SIZE(MEDIUM)
    TIMEOUT(600)
ENDIF()

SRCS(
    main.cpp
)

PEERDIR(
    contrib/ydb/core/base
    contrib/ydb/core/blobstorage
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/dsproxy/mock
    contrib/ydb/core/mind/bscontroller
    contrib/ydb/core/protos
    contrib/ydb/core/testlib/default
    contrib/ydb/core/testlib/basics
)

YQL_LAST_ABI_VERSION()

REQUIREMENTS(network:full)

END()
