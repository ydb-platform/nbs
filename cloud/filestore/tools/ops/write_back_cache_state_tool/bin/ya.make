PROGRAM(filestore-write-back-cache-state-tool)

SRCS(
    app.cpp
    main.cpp
    options.cpp
)

PEERDIR(
    cloud/filestore/tools/ops/write_back_cache_state_tool/lib
    cloud/storage/core/libs/file_backed_containers
    library/cpp/getopt/small
    library/cpp/protobuf/json
)

END()

RECURSE_FOR_TESTS(
)
