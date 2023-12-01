LIBRARY()

GENERATE_ENUM_SERIALIZATION(checkpoint.h)

SRCS(
    checkpoint.cpp
    checkpoint_light.cpp
    client_state.cpp
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

    library/cpp/actors/core
    library/cpp/containers/intrusive_rb_tree
)

END()

RECURSE_FOR_TESTS(
    ut
)
