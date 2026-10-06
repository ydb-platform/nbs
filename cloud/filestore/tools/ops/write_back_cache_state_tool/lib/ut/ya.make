UNITTEST()

SRCDIR(cloud/filestore/tools/ops/write_back_cache_state_tool/lib)

SRCS(
    state_file_locator_ut.cpp
    state_file_processor_ut.cpp
)

PEERDIR(
    cloud/filestore/tools/ops/write_back_cache_state_tool/lib
    cloud/storage/core/libs/common
    cloud/storage/core/libs/file_backed_containers
    library/cpp/digest/crc32c
)

END()
