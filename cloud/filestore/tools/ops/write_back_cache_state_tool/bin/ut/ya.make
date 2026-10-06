UNITTEST()

SRCDIR(cloud/filestore/tools/ops/write_back_cache_state_tool/bin)

SRCS(
    app.cpp
    app_ut.cpp
    options.cpp
    options_ut.cpp
)

PEERDIR(
    cloud/filestore/tools/ops/write_back_cache_state_tool/lib
    cloud/storage/core/libs/common
    cloud/storage/core/libs/file_backed_containers
    library/cpp/getopt/small
)

END()
