LIBRARY()

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    SRCS(
        server.cpp
    )

    PEERDIR(
        cloud/fastshard/ipc

        contrib/libs/silk/src/fibers
    )
ELSE()
    SRCS(
        server_stub.cpp
    )
ENDIF()

PEERDIR(
    cloud/fastshard/protos
    cloud/fastshard/sn/iface

    cloud/storage/core/libs/common
    cloud/storage/core/protos
)

END()

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
