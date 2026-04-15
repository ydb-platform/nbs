PROGRAM(dirtree)

SRCS(
    app.cpp
    main.cpp
    options.cpp
)

PEERDIR(
    cloud/storage/core/libs/diagnostics

    library/cpp/getopt
)

END()
