PROGRAM(merged-blob-bench)

SRCS(main.cpp)

PEERDIR(
    cloud/blockstore/libs/storage/partition/model
    cloud/blockstore/libs/storage/protos_ydb
    contrib/ydb/core/base
    contrib/libs/protobuf
    cloud/blockstore/libs/diagnostics
    contrib/libs/lz4
    contrib/libs/snappy
    contrib/libs/zstd
    contrib/libs/fastlz
)

END()
