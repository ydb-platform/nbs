OWNER(g:cloud-nbs)

GO_LIBRARY()

SRCS(
    clear_deleted_images_task.go
    common.go
    create_image_from_disk_task.go
    create_image_from_image_task.go
    create_image_from_snapshot_task.go
    create_image_from_url_task.go
    delete_image_task.go
    interface.go
    register.go
    service.go
)

END()

RECURSE(
    config
    protos
)

RECURSE_FOR_TESTS(
    mocks
)
