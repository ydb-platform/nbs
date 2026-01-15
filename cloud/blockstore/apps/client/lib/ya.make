LIBRARY()

SRCS(
    alter_placement_group_membership.cpp
    alter_volume.cpp
    app.cpp
    assign_volume.cpp
    backup_volume.cpp
    bootstrap.cpp
    check_range.cpp
    command.cpp
    create_checkpoint.cpp
    create_placement_group.cpp
    create_volume.cpp
    create_volume_from_device.cpp
    create_volume_link.cpp
    delete_checkpoint.cpp
    describe_disk_registry_config.cpp
    describe_endpoint.cpp
    describe_placement_group.cpp
    describe_volume.cpp
    describe_volume_model.cpp
    destroy_placement_group.cpp
    destroy_volume.cpp
    destroy_volume_link.cpp
    discover_instances.cpp
    execute_action.cpp
    factory.cpp
    get_changed_blocks.cpp
    get_checkpoint_status.cpp
    kick_endpoint.cpp
    list_endpoints.cpp
    list_keyrings.cpp
    list_placement_groups.cpp
    list_volumes.cpp
    ping.cpp
    query_agents_info.cpp
    query_available_storage.cpp
    read_blocks.cpp
    refresh_endpoint.cpp
    resize_volume.cpp
    resume_device.cpp
    start_endpoint.cpp
    stat_volume.cpp
    stop_endpoint.cpp
    update_disk_registry_config.cpp
    update_volume_throttling_config.cpp
    volume_manipulation_params.cpp
    write_blocks.cpp
    zero_blocks.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/public/api/protos

    cloud/blockstore/libs/client
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/encryption
    cloud/blockstore/libs/encryption/model
    cloud/blockstore/libs/service

    cloud/storage/core/libs/common
    cloud/storage/core/libs/version

    library/cpp/getopt/small
    library/cpp/lwtrace/mon
    library/cpp/protobuf/util
    library/cpp/threading/blocking_queue
    library/cpp/json

    contrib/ydb/library/actors/util
)

END()

RECURSE_FOR_TESTS(ut)
