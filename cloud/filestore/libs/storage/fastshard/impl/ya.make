SET(FORCE_FASTSHARD_IPC_STUB YES)

RECURSE(
    mem
    naive_mirrored
)

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    RECURSE(
        model
    )

    RECURSE_FOR_TESTS(
        bench
    )
ENDIF()
