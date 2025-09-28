DLL(filestore-fsdev)

SRCS(
    bootstrap.cpp
    logging.cpp
    malloc_info.cpp
    module.cpp
)

ADDINCL(
    cloud/filestore/apps/fsdev/spdk/include
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    library/cpp/malloc/api
)

END()
