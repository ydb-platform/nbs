LIBRARY()

#INCLUDE(${ARCADIA_ROOT}/cloud/storage/deny_ydb_dependency.inc)

GENERATE_ENUM_SERIALIZATION(checkpoint.h)

SRCS(
    checkpoint.cpp
    checkpoint_light.cpp
    client_state.cpp
    helpers.cpp
    merge.cpp
    meta.cpp
    requests_inflight.cpp
    retry_policy.cpp
    stripe.cpp
    volume_params.cpp
    volume_throttler_logger.cpp
    volume_throttling_policy.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/service
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/protos

    cloud/storage/core/libs/throttling

    contrib/ydb/library/actors/core
    library/cpp/containers/intrusive_rb_tree
)

END()

RECURSE_FOR_TESTS(
    ut
)
