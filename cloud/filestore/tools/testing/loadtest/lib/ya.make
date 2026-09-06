LIBRARY(filestore-testing-loadtest-lib)

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    SRCS(
        request_fastshard.cpp
    )

    PEERDIR(
        contrib/libs/silk/src/fibers
    )
ELSE()
    SRCS(
        request_fastshard_stub.cpp
    )
ENDIF()

SRCS(
    client.cpp
    context.h
    executor.cpp
    request_data.cpp
    request_index.cpp
    request_datashard_like.cpp
    request_replay_fs.cpp
    request_replay_grpc.cpp
    request_replay.cpp
    shm_client.cpp
    test.cpp
)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/libs/client
    cloud/filestore/libs/storage/fastshard/client
    cloud/filestore/libs/diagnostics/events
    cloud/filestore/libs/service
    cloud/filestore/libs/service_local
    cloud/filestore/private/api/protos
    cloud/filestore/public/api/protos
    cloud/filestore/tools/analytics/libs/event-log
    cloud/filestore/tools/testing/loadtest/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    library/cpp/aio
    library/cpp/testing/unittest
)

END()
