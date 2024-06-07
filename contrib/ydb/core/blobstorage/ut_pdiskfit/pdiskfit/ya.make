IF (OS_LINUX)
    PROGRAM(pdiskfit)

    SRCS(
        pdiskfit.cpp
    )

    PEERDIR(
        contrib/ydb/apps/version
        library/cpp/getopt
        library/cpp/string_utils/parse_size
        contrib/ydb/core/blobstorage
        contrib/ydb/core/blobstorage/ut_pdiskfit/lib
        contrib/ydb/core/mon
    )

    END()
ENDIF()
