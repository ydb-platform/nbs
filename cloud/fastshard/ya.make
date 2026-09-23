RECURSE(
    bootstrap
    ipc
    journal
    protos
    sn
    testlib
)

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    RECURSE(
        client
        loadtest
    )
ENDIF()
