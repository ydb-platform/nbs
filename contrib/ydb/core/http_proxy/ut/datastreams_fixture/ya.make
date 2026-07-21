LIBRARY()


PEERDIR(
    contrib/restricted/nlohmann_json
    library/cpp/resource
    library/cpp/http/misc
    library/cpp/xml/document
    contrib/ydb/core/base
    contrib/ydb/core/http_proxy
    contrib/ydb/core/testlib/default
    contrib/ydb/core/tx/datashard/ut_common
    contrib/ydb/library/aclib
    contrib/ydb/library/actors/http
    contrib/ydb/library/grpc/server
    contrib/ydb/library/grpc/server/actors
    contrib/ydb/library/persqueue/tests
    contrib/ydb/library/testlib/service_mocks
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/sql/pg_dummy
    contrib/ydb/public/sdk/cpp/src/client/discovery
    contrib/ydb/public/sdk/cpp/src/client/types
    contrib/ydb/services/datastreams
    contrib/ydb/services/kesus
    contrib/ydb/services/persqueue_cluster_discovery
    contrib/ydb/services/ydb
    contrib/ydb/services/ymq
)

SRCS(
    datastreams_fixture.cpp
    sqs_xml_ut_helpers.cpp
)

YQL_LAST_ABI_VERSION()

END()

