PROGRAM()

PEERDIR(
    library/cpp/getopt

    cloud/storage/core/libs/common
    cloud/storage/core/libs/aio
    contrib/libs/libaio
    contrib/libs/liburing
)

SRCS(
    main.cpp
    options.cpp
)

END()
