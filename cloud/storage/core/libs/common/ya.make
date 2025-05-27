LIBRARY()

GENERATE_ENUM_SERIALIZATION(error.h)

SRCS(
    affinity.cpp
    aligned_buffer.cpp
    alloc.cpp
    backoff_delay_provider.cpp
    block_buffer.cpp
    block_data_ref.cpp
    byte_vector.cpp
    compressed_bitmap.cpp
    concurrent_queue.cpp
    context.cpp
    error.cpp
    file_io_service.cpp
    file_ring_buffer.cpp
    format.cpp
    guarded_sglist.cpp
    helpers.cpp
    history.cpp
    lru_cache.cpp
    media.cpp
    page_size.cpp
    persistent_table.cpp
    proto_helpers.cpp
    random.cpp
    ring_buffer.cpp
    scheduler.cpp
    scheduler_test.cpp
    scoped_handle.cpp
    sglist_block_range.cpp
    sglist.cpp
    sglist_iter.cpp
    sglist_test.cpp
    sleeper.cpp
    sleeper_test.cpp
    startable.cpp
    task_queue.cpp
    thread.cpp
    thread_park.cpp
    thread_pool.cpp
    timer.cpp
    timer_test.cpp
    verify.cpp
)

PEERDIR(
    cloud/storage/core/protos

    library/cpp/deprecated/atomic
    library/cpp/digest/crc32c
    library/cpp/json/writer
    library/cpp/logger
    library/cpp/lwtrace
    library/cpp/protobuf/util
    library/cpp/threading/future

    contrib/ydb/library/actors/prof
)

END()

RECURSE_FOR_TESTS(
    bench
    ut
)
