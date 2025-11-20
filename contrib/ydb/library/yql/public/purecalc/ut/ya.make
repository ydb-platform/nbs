UNITTEST()

TAG(ya:manual)

SRCS(
    empty_stream.h
    fake_spec.cpp
    fake_spec.h
    test_schema.cpp
    test_sexpr.cpp
    test_sql.cpp
    test_pg.cpp
    test_udf.cpp
    test_user_data.cpp
    test_eval.cpp
    test_pool.cpp
)

PEERDIR(
    contrib/ydb/library/yql/public/purecalc
    contrib/ydb/library/yql/public/purecalc/io_specs/protobuf
    contrib/ydb/library/yql/public/purecalc/ut/protos
)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

END()
