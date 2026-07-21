UNITTEST()

SRCS(
    empty_stream.h
    fake_spec.cpp
    fake_spec.h
    test_compile_settings.cpp
    test_langver.cpp
    test_linear.cpp
    test_schema.cpp
    test_sexpr.cpp
    test_sql.cpp
    test_pg.cpp
    test_udf.cpp
    test_user_data.cpp
    test_eval.cpp
    test_fatal_err.cpp
    test_pool.cpp
    test_runtime_settings.cpp
    test_mixed_allocators.cpp
)

PEERDIR(
    contrib/ydb/library/yql/public/purecalc
    contrib/ydb/library/yql/public/purecalc/io_specs/protobuf
    contrib/ydb/library/yql/public/purecalc/ut/protos
    contrib/ydb/library/yql/public/purecalc/helpers/stream
)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

END()
