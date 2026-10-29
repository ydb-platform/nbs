LIBRARY()

SRCS(
    error.cpp
)

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    SRCS(
        context.cpp
    )

    PEERDIR(
        contrib/libs/silk/src/fibers
    )
ENDIF()

END()
