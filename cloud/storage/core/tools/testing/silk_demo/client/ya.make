PROGRAM(silk-demo-client)

CXXFLAGS(-std=c++20)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/storage/core/tools/testing/silk_demo/protos

    library/cpp/getopt/small
)

END()
