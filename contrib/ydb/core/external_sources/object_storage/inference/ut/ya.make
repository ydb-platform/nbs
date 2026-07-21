GTEST()

PEERDIR(
    contrib/ydb/library/yql/public/udf/service/stub
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/core/external_sources/object_storage/inference
    contrib/ydb/core/external_sources/object_storage
    contrib/ydb/core/tx/scheme_board
    contrib/ydb/library/yql/providers/common/http_gateway/mock
    contrib/ydb/core/util/actorsys_test
)

SRCS(
    arrow_inference_ut.cpp
)

END()
