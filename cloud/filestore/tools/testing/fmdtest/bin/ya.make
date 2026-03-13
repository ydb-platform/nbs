PROGRAM(fmdtest)

SRCS(
    app.cpp
    main.cpp
    options.cpp
)

PEERDIR(
    cloud/storage/core/libs/diagnostics

    library/cpp/getopt
    library/cpp/json

    util
)

END()
