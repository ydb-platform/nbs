UNITTEST()

SIZE(MEDIUM)

FORK_SUBTESTS()

PEERDIR(
    contrib/restricted/nlohmann_json
    library/cpp/string_utils/url
    contrib/ydb/library/actors/http
    contrib/ydb/library/grpc/server
    contrib/ydb/library/grpc/server/actors
    contrib/ydb/core/base
    contrib/ydb/core/http_proxy
    contrib/ydb/core/http_proxy/ut/datastreams_fixture
    contrib/ydb/core/metering
    contrib/ydb/core/persqueue/ut/common
    contrib/ydb/core/quoter/public
    contrib/ydb/core/testlib/default
    contrib/ydb/public/sdk/cpp/src/library/kafka
    contrib/ydb/library/aclib
    contrib/ydb/library/persqueue/tests
    contrib/ydb/public/sdk/cpp/src/client/discovery
    contrib/ydb/public/sdk/cpp/src/client/topic
    contrib/ydb/public/sdk/cpp/src/client/types
    contrib/ydb/services/sqs_topic
    contrib/ydb/services/ydb
)

SRCS(
    ../sqs_topic_ut.cpp
    ../sqs_topic_cdc_ut.cpp
    ../sqs_topic_xml_ut.cpp
    inside_ydb_ut.cpp
)

ENV(INSIDE_YDB="1")

YQL_LAST_ABI_VERSION()

END()
