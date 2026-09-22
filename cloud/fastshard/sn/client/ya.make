LIBRARY()

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    SRCS(
        client.cpp
    )

    PEERDIR(
        cloud/fastshard/ipc

        contrib/libs/silk/src/fibers
    )
ELSE()
    SRCS(
        client_stub.cpp
    )
ENDIF()

PEERDIR(
    cloud/fastshard/sn/iface

    cloud/storage/core/libs/common
    cloud/storage/core/protos
)

END()

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    RECURSE_FOR_TESTS(
        bench
        ut
    )
ENDIF()
