PROGRAM()

SRCS(
    main.cpp
    options.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/libs/hive_proxy/protos
    cloud/storage/core/libs/ss_proxy/protos

    library/cpp/getopt
)

END()
