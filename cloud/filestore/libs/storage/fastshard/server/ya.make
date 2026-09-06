LIBRARY()

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    SRCS(
        server.cpp
    )

    PEERDIR(
        cloud/filestore/libs/storage/fastshard/ipc

        contrib/libs/silk/src/fibers
    )
ELSE()
    SRCS(
        server_stub.cpp
    )
ENDIF()

PEERDIR(
    cloud/filestore/libs/storage/fastshard/iface
    cloud/filestore/libs/storage/fastshard/server/protos

    cloud/storage/core/libs/common

    library/cpp/threading/future
)

END()

# TODO(#5895): fix silk bootstrap/shutdown under msan
IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB AND SANITIZER_TYPE != "memory")
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
