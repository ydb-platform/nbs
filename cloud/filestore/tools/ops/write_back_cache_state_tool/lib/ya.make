LIBRARY()

SRCS(
    state_file_locator.cpp
    state_file_processor.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/protos
    cloud/filestore/tools/ops/write_back_cache_state_tool/protos
)

END()

RECURSE_FOR_TESTS(
    ut
)
