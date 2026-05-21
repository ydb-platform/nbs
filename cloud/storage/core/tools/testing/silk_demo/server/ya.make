PROGRAM(silk-demo-server)

CXXFLAGS(-std=c++20)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/storage/core/tools/testing/silk_demo/protos

    contrib/libs/silk/src/fibers
    contrib/libs/silk/src/util

    library/cpp/getopt/small
)

END()
