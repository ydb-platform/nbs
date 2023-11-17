PROGRAM()

SRCS(
    ext4-meta-reader.cpp

    main.cpp
)

PEERDIR(
    cloud/storage/core/libs/common

    library/cpp/regex/pcre
    library/cpp/getopt/small
)


END()

RECURSE_FOR_TESTS(ut)
