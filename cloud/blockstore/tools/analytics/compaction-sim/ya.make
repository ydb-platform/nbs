PROGRAM(blockstore-compaction-sim)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics/events
    cloud/blockstore/libs/service
    cloud/blockstore/libs/storage/partition/model
    
    cloud/storage/core/libs/common

    library/cpp/getopt
    library/cpp/eventlog/dumper
    library/cpp/logger
    library/cpp/sighandler
)

SRCS(
    main.cpp
)

END()
