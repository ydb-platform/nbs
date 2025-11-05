GTEST()

PEERDIR(
    contrib/ydb/core/external_sources/object_storage/inference
    contrib/ydb/core/external_sources/object_storage
    contrib/ydb/core/tx/scheme_board
    contrib/ydb/library/yql/providers/common/http_gateway/mock
)

SRCS(
    arrow_inference_ut.cpp
)

END()
