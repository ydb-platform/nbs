LIBRARY()

GENERATE_ENUM_SERIALIZATION(error.h)

SRCS(
    affinity.cpp
    alloc.cpp
    backoff_delay_provider.cpp
    byte_vector.cpp
    compressed_bitmap.cpp
    concurrent_queue.cpp
    context.cpp
    error.cpp
    file_io_service.cpp
    format.cpp
    helpers.cpp
    media.cpp
    proto_helpers.cpp
    random.cpp
    ring_buffer.cpp
    scheduler.cpp
    scheduler_test.cpp
    scoped_handle.cpp
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
    library/cpp/actors/prof
    library/cpp/deprecated/atomic
    library/cpp/logger
    library/cpp/lwtrace
    library/cpp/protobuf/util
    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(
    bench
    ut
)
