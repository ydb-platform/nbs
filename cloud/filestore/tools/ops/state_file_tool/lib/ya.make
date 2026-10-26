LIBRARY()

SRCS(
    state_file_locator.cpp
)

PEERDIR(
    cloud/filestore/tools/ops/state_file_tool/protos
    cloud/storage/core/libs/common
)

END()

RECURSE_FOR_TESTS(
    ut
)
