RECURSE(
    client
    iface
    impl
    quorum
    server
)

IF (OPENSOURCE AND NOT FORCE_FASTSHARD_IPC_STUB)
    RECURSE(
        fastshard_client
    )
ENDIF()
