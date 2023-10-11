OWNER(g:cloud-nbs)

GO_TEST_FOR(cloud/disk_manager/internal/pkg/services/filesystem)

GO_TEST_SRCS(
    create_filesystem_task_test.go
    delete_filesystem_task_test.go
)

END()
