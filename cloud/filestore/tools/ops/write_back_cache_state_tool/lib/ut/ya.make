UNITTEST()

SRCDIR(cloud/filestore/tools/ops/write_back_cache_state_tool/lib)

SRCS(
    state_file_locator_ut.cpp
)

PEERDIR(
    cloud/filestore/tools/ops/write_back_cache_state_tool/lib
    cloud/storage/core/libs/file_backed_containers
)

END()
