LIBRARY()

SRCS()

PEERDIR(
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/protos
    cloud/blockstore/libs/storage/protos_ydb

    library/cpp/actors/core
    library/cpp/lwtrace
)

END()

RECURSE()

RECURSE_FOR_TESTS(
    ut
)
