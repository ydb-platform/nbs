LIBRARY()

SRCS(
    action_graph.cpp
    aliased_volumes.cpp
    app_context.cpp
    app.cpp
    bootstrap.cpp
    buffer_pool.cpp
    client_factory.cpp
    compare_data_action_runner.cpp
    control_plane_action_runner.cpp
    countdown_latch.cpp
    filesystem_client.cpp
    helpers.cpp
    load_test_runner.cpp
    options.cpp
    range_allocator.cpp
    range_map.cpp
    request_generator.cpp
    suite_runner.cpp
    test_runner.cpp
    validation_callback.cpp
    volume_infos.cpp
)

PEERDIR(
    cloud/blockstore/config
    cloud/blockstore/libs/client
    cloud/blockstore/libs/client_rdma
    cloud/blockstore/libs/client_spdk
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/nbd
    cloud/blockstore/libs/rdma_test
    cloud/blockstore/libs/rdma/iface
    cloud/blockstore/libs/rdma/impl
    cloud/blockstore/libs/service
    cloud/blockstore/libs/spdk/iface
    cloud/blockstore/libs/validation
    cloud/blockstore/private/api/protos
    cloud/blockstore/tools/testing/loadtest/protos

    cloud/storage/core/libs/grpc
    cloud/storage/core/libs/endpoints/iface
    cloud/storage/core/libs/endpoints/fs
    cloud/storage/core/libs/version

    library/cpp/aio
    library/cpp/containers/concurrent_hash
    library/cpp/deprecated/atomic
    library/cpp/eventlog
    library/cpp/eventlog/dumper
    library/cpp/getopt
    library/cpp/histogram/hdr
    library/cpp/json
    library/cpp/logger
    library/cpp/protobuf/json
    library/cpp/protobuf/util
    library/cpp/sighandler
    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(
    ut
)
