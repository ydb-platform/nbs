PROGRAM()

SRCS(
    kqprun.cpp
)

PEERDIR(
    library/cpp/getopt

    contrib/ydb/tests/tools/kqprun/src
)

PEERDIR(
    contrib/ydb/library/yql/udfs/common/datetime2
    contrib/ydb/library/yql/udfs/common/string
    contrib/ydb/library/yql/udfs/common/yson2
)

YQL_LAST_ABI_VERSION()

END()
