LIBRARY()

SRCS(
    auth_provider_kikimr.cpp
    service_kikimr.cpp
)

PEERDIR(
    cloud/blockstore/config

    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/service
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core

    cloud/storage/core/libs/auth

    ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(ut)
