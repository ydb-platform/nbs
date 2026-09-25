LIBRARY()

SRCS(
    state_file_locator.cpp
    state_file_processor.cpp
)

PEERDIR(
    cloud/filestore/libs/vfs_fuse/write_back_cache
    cloud/filestore/tools/ops/write_back_cache_state_tool/protos
    cloud/storage/core/libs/common
    cloud/storage/core/libs/file_backed_containers
    cloud/storage/core/protos
    library/cpp/digest/crc32c
)

END()

RECURSE_FOR_TESTS(
    ut
)
