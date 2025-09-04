PROGRAM()

SRCS(
    main.cpp
    commands.cpp
    cmd_drop.cpp
    cmd_prepare.cpp
    cmd_prepare_scheme.cpp
    cmd_run_query.cpp
    cmd_run_bench.cpp
)

PEERDIR(
    contrib/ydb/core/kqp/tests/tpch/lib
    library/cpp/json
    contrib/ydb/public/lib/ydb_cli/commands
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/public/lib/yson_value
)

END()

RECURSE(
    lib
)
