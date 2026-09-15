UNITTEST_FOR(cloud/storage/core/libs/journalled_device)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    device_page_store_ut.cpp
    device_ut.cpp
    journal_ut.cpp
    journalled_device_ut.cpp
    journalled_device_v2_ut.cpp
    key_buffer_store_ut.cpp
    log_chain_ut.cpp
    log_index_ut.cpp
    log_record_ut.cpp
    lsn_barrier_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/threading/future
)

END()
