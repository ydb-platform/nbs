PROGRAM()

PEERDIR(
    library/cpp/getopt
    library/cpp/json

    cloud/storage/core/libs/common
)

SRCS(
    main.cpp
    histogram.cpp
    thread_pool.cpp
)

END()
