LIBRARY()

SRCS(
    printer.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    contrib/ydb/library/formats/arrow/protos
    contrib/ydb/library/yql/public/issue/protos
    contrib/ydb/library/yql/public/types
)

YQL_LAST_ABI_VERSION()

END()
