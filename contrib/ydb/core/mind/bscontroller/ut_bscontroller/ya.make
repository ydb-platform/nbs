UNITTEST()

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
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
