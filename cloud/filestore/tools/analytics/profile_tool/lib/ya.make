LIBRARY()

SRCS(
    command.cpp
    common_filter_params.cpp
    dump_events.cpp
    factory.cpp
    find_bytes_access.cpp
    mask.cpp
)

PEERDIR(
    cloud/filestore/libs/diagnostics/events
    cloud/filestore/tools/analytics/libs/event-log
    cloud/storage/core/libs/systemd_time

    library/cpp/eventlog/dumper
    library/cpp/getopt
    library/cpp/json
)

END()

RECURSE_FOR_TESTS(
    ut
)
