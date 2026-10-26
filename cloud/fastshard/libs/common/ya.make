LIBRARY()

SRCS(
    context.cpp
    error.cpp
)

IF (NOT OPENSOURCE OR FORCE_FASTSHARD_IPC_STUB)
    CFLAGS(GLOBAL -DFASTSHARD_IPC_STUB)
ELSE()
    PEERDIR(
        contrib/libs/silk/src/fibers
    )
ENDIF()

END()
