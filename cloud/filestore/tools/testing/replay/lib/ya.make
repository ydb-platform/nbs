LIBRARY(filestore-testing-replay-lib)

SRCS(
    client.cpp
    context.h
    executor.cpp
    request_replay_fs.cpp
    request_replay_grpc.cpp
    test.cpp
)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/public/api/protos

    cloud/filestore/libs/client
    cloud/filestore/libs/service

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    cloud/filestore/tools/testing/replay/protos

    library/cpp/deprecated/atomic

    library/cpp/eventlog
    cloud/filestore/libs/diagnostics/events
    cloud/filestore/tools/analytics/libs/event-log
    library/cpp/testing/unittest

    library/cpp/aio
)


END()
