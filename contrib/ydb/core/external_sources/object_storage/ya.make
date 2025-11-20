RECURSE(
    inference
)

LIBRARY()

PEERDIR(
    contrib/libs/apache/arrow
    contrib/libs/curl

    contrib/ydb/core/fq/libs/config/protos
    contrib/ydb/library/actors/core
    contrib/ydb/library/yql/providers/common/http_gateway
    contrib/ydb/library/yql/providers/s3/credentials
)

SRC(
    s3_fetcher.cpp
)

END()
