OWNER(g:cloud-nbs)

GO_LIBRARY()

SRCS(
    consts.go
    interface.go
    register.go
    service.go

    acquire_base_disk_task.go
    clear_deleted_base_disks_task.go
    clear_released_slots_task.go
    configure_pool_task.go
    create_base_disk_task.go
    delete_base_disks_task.go
    delete_pool_task.go
    image_deleting_task.go
    optimize_base_disks_task.go
    rebase_overlay_disk_task.go
    release_base_disk_task.go
    retire_base_disk_task.go
    retire_base_disks_task.go
    schedule_base_disks_task.go
)

GO_TEST_SRCS(
    acquire_base_disk_task_test.go
    configure_pool_task_test.go
    optimize_base_disks_task_test.go
    release_base_disk_task_test.go
)

END()

RECURSE(
    config
    protos
    storage
)

RECURSE_FOR_TESTS(
    mocks
    tests
)
