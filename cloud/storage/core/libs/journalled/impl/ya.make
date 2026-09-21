LIBRARY()

SRCS(
    device_helpers.cpp
    device_page_store.cpp
    file_device.cpp
    journal.cpp
    journalled_device_v1.cpp
    journalled_device_v2.cpp
    key_buffer_store.cpp
    log_chain.cpp
    log_index.cpp
    log_record.cpp
    lsn_barrier.cpp
    memory_device.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/libs/coroutine
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/journalled/iface
    cloud/storage/core/protos
    library/cpp/digest/crc32c
)

END()

RECURSE_FOR_TESTS(
    ut
)
