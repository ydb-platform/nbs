PROGRAM(blockstore-dump-event-log)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/tools/analytics/libs/event-log

    contrib/libs/sqlite3
    library/cpp/eventlog/dumper
)

SRCS(
    io_deps_stat_accumulator.cpp
    main.cpp
    profile_log_event_handler.cpp
    read_write_requests_with_inflight.cpp
    sqlite_output.cpp
    main.cpp
)

END()
