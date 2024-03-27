PROGRAM(blockstore-bad-guest)

SRCS(
    app.cpp
    main.cpp
    options.cpp
)

PEERDIR(
    cloud/blockstore/tools/testing/bad-guest/lib

    library/cpp/getopt
    library/cpp/threading/future
)

END()
