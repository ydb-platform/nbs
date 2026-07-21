UNITTEST_FOR(contrib/ydb/library/actors/queues)

SRCS(
    mpmc_ring_queue_ut_single_thread.cpp
    mpmc_ring_queue_ut_multi_threads.cpp

    mpmc_ring_queue_v1_ut_single_thread.cpp

    mpmc_ring_queue_v3_ut_single_thread.cpp
    mpmc_ring_queue_v3_ut_multi_threads.cpp

    mpmc_bitmap_buffer_ut.cpp
)

PEERDIR(
    contrib/ydb/library/actors/queues/observer
)

END()
