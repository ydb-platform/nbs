LIBRARY()

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    SRCS(
        async_client.cpp
        client.cpp
    )

    PEERDIR(
        cloud/filestore/libs/storage/fastshard/ipc

        contrib/libs/silk/src/fibers
    )
ELSE()
    SRCS(
        async_client_stub.cpp
        client_stub.cpp
    )
ENDIF()

PEERDIR(
    cloud/filestore/libs/storage/fastshard/server/protos

    cloud/storage/core/libs/common

    library/cpp/threading/future
)

END()
