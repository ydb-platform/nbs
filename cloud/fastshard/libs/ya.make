RECURSE(
    bootstrap
    common
    ipc
    journal
    sn
    storage_group
)

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    RECURSE(
        fiber
    )
ENDIF()
