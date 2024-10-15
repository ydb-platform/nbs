PROGRAM(unsensitivifier)

PEERDIR(
    cloud/filestore/libs/diagnostics/events
    library/cpp/digest/md5
    library/cpp/eventlog
    library/cpp/getopt/small
    library/cpp/sighandler
)

SRCS(
    app.cpp
    bootstrap.cpp
    main.cpp
    options.cpp
    unsensitivifier.cpp
)

END()


