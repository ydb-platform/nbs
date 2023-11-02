DLL(blockstore-plugin)
EXPORTS_SCRIPT(plugin.symlist)

SRCS(
    bootstrap.cpp
    logging.cpp
    malloc_info.cpp
    plugin.cpp
)

PEERDIR(
    cloud/vm/blockstore/lib

    cloud/blockstore/libs/client
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/discovery

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics
    cloud/storage/core/libs/grpc
    cloud/storage/core/libs/version

    library/cpp/lwtrace
    library/cpp/malloc/api
    library/cpp/protobuf/util
)

END()
