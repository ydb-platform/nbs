UNITTEST_FOR(cloud/fastshard/journal/impl)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    device_page_store_ut.cpp
    file_device_ut.cpp
    journalled_device_v1_ut.cpp
    journalled_device_v2_ut.cpp
    key_buffer_store_ut.cpp
    log_chain_ut.cpp
    log_index_ut.cpp
    log_record_ut.cpp
    lsn_barrier_ut.cpp
    memory_device_ut.cpp
)

PEERDIR(
    cloud/storage/core/libs/aio

    library/cpp/testing/unittest
    library/cpp/threading/future
)

END()
