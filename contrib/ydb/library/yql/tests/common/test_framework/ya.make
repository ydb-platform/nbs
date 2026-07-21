PY23_LIBRARY()

PY_SRCS(
    TOP_LEVEL
    solomon_runner.py
    udf_test_common.py
    yql_utils.py
    yql_ports.py
    yqlrun.py
    yql_http_file_server.py
    test_utils.py
    test_file_common.py
)

PY_SRCS(
    NAMESPACE ydb_library_yql_test_framework
    conftest.py
)

PEERDIR(
    contrib/python/requests
    contrib/python/six
    contrib/python/urllib3
    library/python/cyson
    library/python/resource
    library/python/port_manager
    contrib/ydb/library/yql/core/file_storage/proto
    contrib/ydb/library/yql/providers/common/proto
)

RESOURCE(
    contrib/ydb/library/yql/data/language/features.json contrib/ydb/library/yql/data/language/features.json
    contrib/ydb/library/yql/data/language/langver.json contrib/ydb/library/yql/data/language/langver.json
)

END()

RECURSE(
    udfs_deps
)
