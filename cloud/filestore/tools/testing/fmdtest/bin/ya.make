PROGRAM(fmdtest)

SRCS(
    app.cpp
    dir_state.cpp
    main.cpp
    multi_client.cpp
    options.cpp
    sync.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/libs/diagnostics

    library/cpp/getopt
    library/cpp/json
)

END()
