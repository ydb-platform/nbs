GO_LIBRARY()

SRCS(
    disk_service.go
    image_service.go
    interceptor.go
    filesystem_service.go
    operation_service.go
    placement_group_service.go
    private_service.go
    snapshot_service.go
)

END()

RECURSE_FOR_TESTS(
    disk_service_nemesis_test
    disk_service_test
    disk_service_max_free_bytes_policy_nemesis_test
    disk_service_max_free_bytes_policy_test
    facade_test
    image_service_nemesis_test
    image_service_test
    filesystem_service_nemesis_test
    filesystem_service_test
    placement_group_service_nemesis_test
    placement_group_service_test
    private_service_nemesis_test
    private_service_test
    with_broken_checkpoints_test
    with_broken_checkpoints_nemesis_test
    snapshot_service_nemesis_test
    snapshot_service_test
    testcommon
)
