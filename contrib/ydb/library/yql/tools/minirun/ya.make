PROGRAM()

ALLOCATOR(J)

SRCS(
    minirun.cpp
)

IF (OS_LINUX)
    # prevent external python extensions to lookup protobuf symbols (and maybe
    # other common stuff) in main binary
    EXPORTS_SCRIPT(${ARCADIA_ROOT}/contrib/ydb/library/yql/tools/exports.symlist)
ENDIF()

PEERDIR(
    contrib/ydb/library/yql/tools/yql_facade_run
    contrib/ydb/library/yql/providers/pure
    contrib/ydb/library/yql/providers/common/provider
    contrib/ydb/library/yql/minikql/invoke_builtins/llvm16
    contrib/ydb/library/yql/minikql/comp_nodes/llvm16
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/public/udf/service/terminate_policy
    contrib/ydb/library/yql/sql/pg
)

YQL_LAST_ABI_VERSION()

RESOURCE(
    contrib/ydb/library/yql/cfg/tests/gateways.conf gateways.conf
    contrib/ydb/library/yql/cfg/tests/fs.conf fs.conf
    contrib/ydb/library/yql/cfg/tests/fs_arc.conf fs_arc.conf
    contrib/ydb/library/yql/cfg/tests/fs_http.conf fs_http.conf
)

END()
