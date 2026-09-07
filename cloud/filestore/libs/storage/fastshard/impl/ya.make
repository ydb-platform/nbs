RECURSE(
    mem
    naive_mirrored
)

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    RECURSE(
        model
        fiber_bridge
    )

    RECURSE_FOR_TESTS(
        bench
    )
ENDIF()
