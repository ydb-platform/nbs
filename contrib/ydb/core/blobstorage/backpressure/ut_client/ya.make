UNITTEST()

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/library/actors/interconnect/mock
    contrib/ydb/core/blobstorage/backpressure
    contrib/ydb/core/blobstorage/base
    contrib/ydb/core/blobstorage/vdisk
    contrib/ydb/core/blobstorage/vdisk/common
    contrib/ydb/core/tx/scheme_board
    contrib/ydb/library/yql/public/udf/service/stub
)

YQL_LAST_ABI_VERSION()

SRCS(
    backpressure_ut.cpp
    defs.h
    loader.h
    skeleton_front_mock.h
)

END()
