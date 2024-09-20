PROGRAM(filestore-replay)

PEERDIR(
    cloud/filestore/config
    cloud/filestore/public/api/protos

    cloud/filestore/libs/client
    cloud/filestore/tools/testing/replay/lib
    cloud/filestore/tools/testing/replay/protos

    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    contrib/ydb/library/actors/util
    library/cpp/getopt
    library/cpp/logger
    library/cpp/protobuf/json
    library/cpp/protobuf/util
    library/cpp/sighandler
)

SRCS(
    app.cpp
    bootstrap.cpp
    main.cpp
    options.cpp
)

END()
