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
    cloud/fastshard/bootstrap
    cloud/fastshard/protos
    cloud/fastshard/sn/client
    cloud/fastshard/sn/iface

    cloud/storage/core/libs/common
    cloud/storage/core/protos

    library/cpp/getopt
    library/cpp/protobuf/util

    contrib/libs/silk/src/fibers
)

END()

RECURSE_FOR_TESTS(
    ut
)
