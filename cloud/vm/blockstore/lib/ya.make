LIBRARY()

SRCS(
    helper.cpp
    plugin.cpp
)

PEERDIR(
    cloud/blockstore/public/api/protos

    cloud/blockstore/libs/client
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/encryption
    cloud/blockstore/libs/nbd
    cloud/blockstore/libs/service

    library/cpp/monlib/dynamic_counters
    library/cpp/protobuf/util
    library/cpp/threading/future

    cloud/vm/api
)

END()

RECURSE_FOR_TESTS(ut)
