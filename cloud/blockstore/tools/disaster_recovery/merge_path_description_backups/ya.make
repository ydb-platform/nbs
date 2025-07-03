PROGRAM()

SRCS(
    main.cpp
    options.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/libs/ss_proxy/protos

    library/cpp/getopt
)

END()
