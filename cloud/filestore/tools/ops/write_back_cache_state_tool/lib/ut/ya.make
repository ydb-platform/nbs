UNITTEST()

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/medium.inc)

SRCDIR(cloud/filestore/tools/ops/write_back_cache_state_tool/lib)

SRCS(
    state_file_processor_ut.cpp
)

PEERDIR(
    cloud/filestore/tools/ops/write_back_cache_state_tool/lib
    cloud/storage/core/libs/file_backed_containers
)

END()
