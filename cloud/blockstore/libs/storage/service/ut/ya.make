UNITTEST_FOR(cloud/blockstore/libs/storage/service)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/medium.inc)

SRCS(
    service_state_ut.cpp
    service_ut_actions.cpp
    service_ut_alter.cpp
    service_ut_create_from_device.cpp
    service_ut_create.cpp
    service_ut_describe_model.cpp
    service_ut_describe.cpp
    service_ut_destroy.cpp
    service_ut_forward.cpp
    service_ut_inactive_clients.cpp
    service_ut_link_volume.cpp
    service_ut_list.cpp
    service_ut_manually_preempted_volumes.cpp
    service_ut_mount.cpp
    service_ut_placement.cpp
    service_ut_read_write.cpp
    service_ut_resume_device.cpp
    service_ut_start.cpp
    service_ut_stats.cpp
    service_ut_update_config.cpp
    service_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/testlib
)

YQL_LAST_ABI_VERSION()

END()
