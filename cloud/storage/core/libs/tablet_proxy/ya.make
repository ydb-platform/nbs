LIBRARY()

SRCS(
    tablet_proxy.cpp
)

PEERDIR(
    cloud/storage/core/libs/actors
    cloud/storage/core/libs/api
    cloud/storage/core/libs/common
    cloud/storage/core/libs/kikimr

    contrib/ydb/core/base
    contrib/ydb/core/protos
    contrib/ydb/core/tablet

    contrib/ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
