UNITTEST_FOR(contrib/ydb/services/ydb)

FORK_SUBTESTS()
SPLIT_FACTOR(60)

REQUIREMENTS(cpu:2)
IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    ydb_bulk_upsert_csv_ut.cpp
    ydb_bulk_upsert_ut.cpp
    ydb_bulk_upsert_olap_ut.cpp
    ydb_coordination_ut.cpp
    ydb_index_table_ut.cpp
    ydb_import_ut.cpp
    ydb_ut.cpp
    ydb_register_node_ut.cpp
    ydb_scripting_ut.cpp
    ydb_table_ut.cpp
    ydb_unary_retry_ut.cpp
    ydb_stats_ut.cpp
    ydb_logstore_ut.cpp
    ydb_olapstore_ut.cpp
    ydb_monitoring_ut.cpp
    ydb_query_ut.cpp
    ydb_read_rows_ut.cpp
    ydb_ldap_login_ut.cpp
    ydb_login_ut.cpp
    ydb_object_storage_ut.cpp
    ydb_whoami_ut.cpp
)

PEERDIR(
    contrib/libs/apache/arrow
    library/cpp/getopt
    contrib/ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/regex/pcre
    library/cpp/svnversion
    contrib/ydb/core/kqp/ut/common
    contrib/ydb/core/testlib/pg
    contrib/ydb/core/tx/datashard/ut_common
    contrib/ydb/core/grpc_services/base
    contrib/ydb/core/testlib
    contrib/ydb/core/security
    contrib/ydb/core/security/ldap_auth_provider/test_utils
    contrib/ydb/library/yql/minikql/dom
    contrib/ydb/library/yql/minikql/jsonpath
    contrib/ydb/library/testlib/service_mocks/ldap_mock
    contrib/ydb/public/lib/experimental
    contrib/ydb/public/lib/json_value
    contrib/ydb/public/lib/yson_value
    contrib/ydb/public/lib/ut_helpers
    contrib/ydb/public/lib/ydb_cli/commands
    contrib/ydb/core/kqp/common/events
    contrib/ydb/core/kqp/common/shutdown
    contrib/ydb/core/kqp/common/simple
    contrib/ydb/public/sdk/cpp/src/client/discovery
    contrib/ydb/public/sdk/cpp/src/client/draft
    contrib/ydb/public/sdk/cpp/src/client/coordination
    contrib/ydb/public/sdk/cpp/src/client/export
    contrib/ydb/public/sdk/cpp/src/client/extension_common
    contrib/ydb/public/sdk/cpp/src/client/operation
    contrib/ydb/public/sdk/cpp/src/client/scheme
    contrib/ydb/public/sdk/cpp/src/client/monitoring
    contrib/ydb/services/ydb
)

YQL_LAST_ABI_VERSION()

END()
