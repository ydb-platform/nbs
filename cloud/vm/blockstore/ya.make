DLL(blockstore-plugin)
EXPORTS_SCRIPT(plugin.symlist)

IF (ARCH_ARM64 AND ALLOCATOR == "TCMALLOC_256K")                                                                                                                           
    # For arm builds with forced -DALLOCATOR=TCMALLOC_256K we should preserve FAKE allocator for DLL
    # Without this plugin will be linked with tcmalloc and cant be used
    DISABLE(COMMON_LINK_SETTINGS)                                                                                                                                              
ENDIF()                                                                                                                                                                    

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

RECURSE(
    lib
)
