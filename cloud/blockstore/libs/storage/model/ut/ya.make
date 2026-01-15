UNITTEST_FOR(cloud/blockstore/libs/storage/model)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    composite_id_ut.cpp
    composite_task_waiter_ut.cpp
    log_prefix_ut.cpp
    log_title_ut.cpp
    request_bounds_tracker_ut.cpp
    requests_in_progress_ut.cpp
    volume_label_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/testlib
)

END()
