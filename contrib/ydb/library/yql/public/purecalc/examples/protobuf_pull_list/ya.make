PROGRAM()

SRCS(
    main.proto
    main.cpp
)

PEERDIR(
    contrib/ydb/library/yql/public/purecalc
    contrib/ydb/library/yql/public/purecalc/io_specs/protobuf
    contrib/ydb/library/yql/public/purecalc/helpers/stream
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    ut
)
