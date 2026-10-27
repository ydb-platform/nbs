LIBRARY()

SRCS(
    acquire_devices.cpp
    advance_lsn_low_watermark.cpp
    app.cpp
    command.cpp
    factory.cpp
    format_device.cpp
    list_devices.cpp
    read_journal_tail.cpp
    read_pages.cpp
    release_devices.cpp
    write_log_record.cpp
)

PEERDIR(
    cloud/fastshard/libs/bootstrap
    cloud/fastshard/libs/sn/client
    cloud/fastshard/libs/sn/iface
    cloud/fastshard/public/api/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/protos

    library/cpp/getopt
    library/cpp/protobuf/util

    contrib/libs/silk/src/fibers
)

END()

RECURSE_FOR_TESTS(
    ut
)
