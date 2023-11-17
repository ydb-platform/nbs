PROGRAM()

SRCS(
    main.cpp

    cleanup.cpp
    parser.cpp
)

PEERDIR(
    cloud/storage/core/libs/common

    library/cpp/getopt/small
)

END()

RECURSE_FOR_TESTS(ut)
