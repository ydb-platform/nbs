LIBRARY()

SRCS(
    log_record.cpp
    log_index.cpp
    log_chain.cpp
    lsn_barriers.cpp
    journal_store.cpp
    journal.cpp
    journalled_device.cpp
)

PEERDIR(
    cloud/storage/core/protos

    cloud/storage/core/libs/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
