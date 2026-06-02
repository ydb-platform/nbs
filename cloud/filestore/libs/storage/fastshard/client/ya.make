LIBRARY()

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    SRCS(
        client.cpp
    )

    PEERDIR(
        contrib/libs/silk/src/fibers
    )
ELSE()
    SRCS(
        client_stub.cpp
    )
ENDIF()

PEERDIR(
    cloud/filestore/libs/storage/fastshard/ipc
    cloud/filestore/libs/storage/fastshard/server/protos
)

END()
