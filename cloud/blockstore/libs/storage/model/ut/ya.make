UNITTEST_FOR(cloud/blockstore/libs/storage/model)

SRCS(
    composite_id_ut.cpp
    composite_task_waiter_ut.cpp
    requests_in_progress_ut.cpp
    recently_written_blocks_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/storage/testlib
)

END()
