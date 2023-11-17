PROGRAM(pd-metadata-bench)

SRCS(
    app.cpp
    main.cpp
    options.cpp
)

PEERDIR(
    cloud/blockstore/tools/testing/pd-metadata-bench/lib

    library/cpp/getopt
    library/cpp/threading/future
)

END()
