PROGRAM()

SRCS(
    main.cpp
    options.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/libs/hive_proxy/protos

    library/cpp/getopt
)

END()
