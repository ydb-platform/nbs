UNITTEST_FOR(cloud/storage/core/libs/journalled_device)

SRCDIR(cloud/storage/core/libs/journalled_device)

SRCS(
    log_record_ut.cpp
    log_index_ut.cpp
    log_chain_ut.cpp
    lsn_barriers_ut.cpp
    journal_store_ut.cpp
    journal_ut.cpp
    journalled_device_ut.cpp
)

PEERDIR(
    cloud/storage/core/libs/journalled_device
)

END()
