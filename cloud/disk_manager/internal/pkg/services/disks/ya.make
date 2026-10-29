GO_LIBRARY()

SRCS(
    alter_disk_task.go
    assign_disk_task.go
    clear_deleted_disks_task.go
    create_disk_common.go
    create_disk_from_image_task.go
    create_disk_from_snapshot_task.go
    create_empty_disk_task.go
    create_overlay_disk_task.go
    delete_disk_task.go
    migrate_disk_task.go
    resize_disk_task.go
    unassign_disk_task.go
    transfer_volume_via_link_task.go
    disk_operation_task.go

    interface.go
    register.go
    service.go
)

GO_TEST_SRCS(
    create_empty_disk_task_test.go
    create_overlay_disk_task_test.go
    delete_disk_task_test.go
    migrate_disk_task_test.go
    transfer_volume_via_link_task_test.go
    service_test.go
    disk_operation_task_test.go
)

END()

RECURSE(
    config
    protos
)

RECURSE_FOR_TESTS(
    mocks
    tests
)
