UNITTEST()

SRCDIR(cloud/filestore/tools/ops/write_back_cache_state_tool/bin)

SRCS(
    options.cpp
    options_ut.cpp
)

PEERDIR(
    library/cpp/getopt/small
)

END()
