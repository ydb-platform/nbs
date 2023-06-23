LIBRARY()

GENERATE_ENUM_SERIALIZATION(auth_counters.h)

SRCS(
    auth_counters.cpp
    authorizer.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/service
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    library/cpp/actors/core
    library/cpp/monlib/dynamic_counters
    ydb/core/base
)

END()

RECURSE_FOR_TESTS(
    ut
)
