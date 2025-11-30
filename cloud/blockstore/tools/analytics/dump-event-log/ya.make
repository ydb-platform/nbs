PROGRAM(blockstore-dump-event-log)

PEERDIR(
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/public/api/protos
    cloud/blockstore/tools/analytics/libs/event-log

    contrib/libs/sqlite3
    library/cpp/eventlog/dumper
)

SRCS(
    dataset_output.cpp
    sqlite_output.cpp
    main.cpp
)

END()

RECURSE_FOR_TESTS(ut)
