RECURSE(
    bootstrap
    common
    ipc
    journal
    sn
)

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    RECURSE(
        fiber
    )
ENDIF()
