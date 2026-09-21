GO_LIBRARY()

SRCS(
    backup_image_task.go
    clear_deleted_images_task.go
    common.go
    create_image_from_disk_task.go
    create_image_from_image_task.go
    create_image_from_snapshot_task.go
    create_image_from_url_task.go
    delete_image_task.go
    interface.go
    register.go
    schedule_backup_image_tasks.go
    service.go
)

GO_TEST_SRCS(
    delete_image_task_test.go
    schedule_backup_image_tasks_test.go
)

END()

RECURSE(
    config
    protos
)

RECURSE_FOR_TESTS(
    mocks
    tasks_tests
    tests
)
