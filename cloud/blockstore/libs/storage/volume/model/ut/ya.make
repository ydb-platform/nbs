UNITTEST_FOR(cloud/blockstore/libs/storage/volume/model)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    checkpoint_light_ut.cpp
    checkpoint_ut.cpp
    client_state_ut.cpp
    merge_ut.cpp
    requests_inflight_ut.cpp
    requests_time_tracker_ut.cpp
    retry_policy_ut.cpp
    stripe_ut.cpp
    volume_params_ut.cpp
    volume_throttling_policy_ut.cpp
)

END()
