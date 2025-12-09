UNITTEST()

SRCS(
    main.cpp
)

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/apps/version
    contrib/ydb/library/actors/interconnect/mock
    library/cpp/testing/unittest
    contrib/ydb/core/blobstorage/backpressure
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/dsproxy
    contrib/ydb/core/blobstorage/groupinfo
    contrib/ydb/core/blobstorage/pdisk/mock
    contrib/ydb/core/blobstorage/vdisk
    contrib/ydb/core/blobstorage/vdisk/common
    contrib/ydb/core/blobstorage/vdisk/repl
    contrib/ydb/core/tx/scheme_board
    contrib/ydb/core/util
    contrib/ydb/core/util/actorsys_test
)

END()
