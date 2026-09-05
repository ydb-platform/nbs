RECURSE(
    mem
    model
    naive_mirrored
)

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    RECURSE(
        fiber_bridge
    )

    RECURSE_FOR_TESTS(
        bench
    )
ENDIF()
