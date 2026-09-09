RECURSE(
    factory
    mem
    hash_table_index
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
