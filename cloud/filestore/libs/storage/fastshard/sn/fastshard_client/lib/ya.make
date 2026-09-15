LIBRARY()

SRCS(
    acquire_devices.cpp
    advance_lsn_low_watermark.cpp
    app.cpp
    command.cpp
    factory.cpp
    read_journal_tail.cpp
    read_pages.cpp
    release_devices.cpp
    write_log_record.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/bootstrap
    cloud/filestore/libs/storage/fastshard/sn/client
    cloud/filestore/libs/storage/fastshard/sn/iface

    cloud/storage/core/libs/common
    cloud/storage/core/protos

    library/cpp/getopt/small
    library/cpp/protobuf/util

    contrib/libs/silk/src/fibers
)

END()

RECURSE_FOR_TESTS(
    ut
)
