LIBRARY()

SRCS(
    fresh_blocks_companion_client.cpp
    fresh_blocks_writer_actor_readblocks.cpp
    fresh_blocks_writer_actor_forward.cpp
    fresh_blocks_writer_actor.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/kikimr
    cloud/blockstore/libs/storage/api
    cloud/blockstore/libs/storage/core
    cloud/blockstore/libs/storage/partition/model
    cloud/blockstore/libs/storage/partition_common
    cloud/blockstore/libs/storage/protos

    cloud/storage/core/libs/api
    cloud/storage/core/libs/common
    cloud/storage/core/libs/tablet
    cloud/storage/core/libs/viewer

    library/cpp/blockcodecs
    library/cpp/cgiparam
    library/cpp/containers/dense_hash
    library/cpp/lwtrace
    library/cpp/monlib/service/pages

    contrib/ydb/core/base
    contrib/ydb/core/blobstorage
    contrib/ydb/core/node_whiteboard
    contrib/ydb/core/scheme
    contrib/ydb/core/tablet
    contrib/ydb/core/tablet_flat
    contrib/ydb/library/actors/core
)

END()

RECURSE_FOR_TESTS(
    ut
)
