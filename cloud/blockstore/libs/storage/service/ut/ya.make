UNITTEST_FOR(cloud/blockstore/libs/storage/service)

FORK_SUBTESTS()
SPLIT_FACTOR(30)

TIMEOUT(600)
SIZE(MEDIUM)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    REQUIREMENTS(
        ram:16
    )
ENDIF()

SRCS(
    service_state_ut.cpp
    service_ut.cpp
    service_ut_actions.cpp
    service_ut_alter.cpp
    service_ut_describe.cpp
    service_ut_describe_model.cpp
    service_ut_create.cpp
    service_ut_create_from_device.cpp
    service_ut_forward.cpp
    service_ut_inactive_clients.cpp
    service_ut_list.cpp
    service_ut_manually_preempted_volumes.cpp
    service_ut_mount.cpp
    service_ut_placement.cpp
    service_ut_read_write.cpp
    service_ut_resume_device.cpp
    service_ut_start.cpp
    service_ut_update_config.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/testlib
)


   YQL_LAST_ABI_VERSION()



END()
