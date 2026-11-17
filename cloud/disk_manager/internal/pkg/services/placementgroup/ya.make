GO_LIBRARY()

SRCS(
    alter_placement_group_membership_task.go
    clear_deleted_placement_groups_task.go
    create_placement_group_task.go
    delete_placement_group_task.go
    interface.go
    register.go
    service.go
)

GO_TEST_SRCS(
    delete_placement_group_disabled_zones_test.go
    placementgroup_test.go
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
