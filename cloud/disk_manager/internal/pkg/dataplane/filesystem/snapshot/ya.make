GO_LIBRARY()

SRCS(
    register.go
    transfer_from_filesystem_to_snapshot_task.go
    transfer_from_snapshot_to_filesystem_task.go
)

END()

RECURSE(
    config
    protos
    storage
)
