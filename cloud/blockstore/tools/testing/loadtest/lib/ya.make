LIBRARY()

SRCS(
    action_graph.cpp
    aliased_volumes.cpp
    app_context.cpp
    buffer_pool.cpp
    client_factory.cpp
    compare_data_action_runner.cpp
    control_plane_action_runner.cpp
    countdown_latch.cpp
    filesystem_client.cpp
    helpers.cpp
    load_test_runner.cpp
    range_allocator.cpp
    range_map.cpp
    request_generator.cpp
    suite_runner.cpp
    test_runner.cpp
    validation_callback.cpp
    volume_infos.cpp
)

PEERDIR(
    cloud/blockstore/tools/testing/loadtest/protos

    cloud/blockstore/libs/client
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/service
    cloud/blockstore/libs/validation
    cloud/blockstore/private/api/protos

    cloud/storage/core/libs/keyring

    library/cpp/aio
    library/cpp/containers/concurrent_hash
    library/cpp/eventlog
    library/cpp/eventlog/dumper
    library/cpp/histogram/hdr
    library/cpp/json
    library/cpp/protobuf/json
    library/cpp/protobuf/util
    library/cpp/threading/future
    library/cpp/deprecated/atomic
)

END()

RECURSE_FOR_TESTS(
    ut
)
