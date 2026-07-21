IF (NOT EXPORT_CMAKE OR NOT OPENSOURCE OR OPENSOURCE_PROJECT != "yt")

PROGRAM()

PEERDIR(
    library/cpp/getopt
    contrib/ydb/library/yql/public/fastcheck
    contrib/ydb/library/yql/public/udf_meta
    library/cpp/colorizer
)

SRCS(
    yql_linter.cpp
)

END()

ENDIF()
