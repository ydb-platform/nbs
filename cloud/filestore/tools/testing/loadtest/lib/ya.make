LIBRARY(filestore-testing-loadtest-lib)

SRCS(
    client.cpp
    context.h
    executor.cpp
    request_data.cpp
    request_index.cpp
    request_replay_fs.cpp
    request_replay_grpc.cpp
    test.cpp
)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/libs/client
    cloud/filestore/libs/diagnostics/events
    cloud/filestore/libs/service
    cloud/filestore/libs/service_local
    cloud/filestore/public/api/protos
    cloud/filestore/tools/analytics/libs/event-log
    cloud/filestore/tools/testing/loadtest/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    library/cpp/aio
    library/cpp/deprecated/atomic
    library/cpp/testing/unittest
)

END()
