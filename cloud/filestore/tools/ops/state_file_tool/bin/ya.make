PROGRAM(filestore-state-file-tool)

SRCS(
    app.cpp
    main.cpp
    options.cpp
)

PEERDIR(
    cloud/filestore/tools/ops/state_file_tool/lib
    cloud/storage/core/libs/common
    cloud/storage/core/libs/file_backed_containers
    library/cpp/getopt/small
)

END()

RECURSE_FOR_TESTS(
    ut
)
