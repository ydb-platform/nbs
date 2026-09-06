LIBRARY()

SET(FORCE_FASTSHARD_IPC_STUB YES)

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    SRCS(
        ipc.cpp
    )

    PEERDIR(
        contrib/libs/silk/src/fibers
    )
ELSE()
    SRCS(
        ipc_stub.cpp
    )
ENDIF()

END()
