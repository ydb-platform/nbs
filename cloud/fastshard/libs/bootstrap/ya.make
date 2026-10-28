LIBRARY()

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    SRCS(
        core.cpp
    )

    PEERDIR(
        contrib/libs/silk/src/fibers
        contrib/libs/silk/src/util
    )
ELSE()
    SRCS(
        core_stub.cpp
    )
ENDIF()

END()
