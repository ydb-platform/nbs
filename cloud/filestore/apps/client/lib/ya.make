LIBRARY(filestore-apps-client-lib)

SRCS(
    add_cluster_node.cpp
    command.cpp
    create.cpp
    create_session.cpp
    describe.cpp
    destroy.cpp
    execute_action.cpp
    factory.cpp
    kick_endpoint.cpp
    list_cluster_nodes.cpp
    list_endpoints.cpp
    list_filestores.cpp
    ls.cpp
    mkdir.cpp
    mount.cpp
    performance_profile_params.cpp
    read.cpp
    remove_cluster_node.cpp
    reset_session.cpp
    resize.cpp
    rm.cpp
    start_endpoint.cpp
    stop_endpoint.cpp
    text_table.cpp
    touch.cpp
    write.cpp
)

PEERDIR(
    cloud/filestore/libs/client
    cloud/filestore/libs/diagnostics
    cloud/filestore/libs/vfs
    cloud/filestore/libs/vfs_fuse

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    contrib/ydb/library/actors/util
    library/cpp/getopt
    library/cpp/logger
    library/cpp/protobuf/json
    library/cpp/threading/future
)

END()

RECURSE_FOR_TESTS(
    ut
)
