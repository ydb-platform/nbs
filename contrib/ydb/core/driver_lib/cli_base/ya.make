LIBRARY(cli_base)

SRCS(
    cli_cmds_db.cpp
    cli_cmds_discovery.cpp
    cli_cmds_root.cpp
    cli_kicli.cpp
)

PEERDIR(
    contrib/ydb/core/driver_lib/cli_config_base
    contrib/ydb/library/aclib
    contrib/ydb/public/lib/deprecated/kicli
    contrib/ydb/public/lib/ydb_cli/common
    contrib/ydb/public/sdk/cpp/client/resources
    contrib/ydb/public/sdk/cpp/client/ydb_table
    contrib/ydb/public/lib/ydb_cli/commands/ydb_discovery
)

YQL_LAST_ABI_VERSION()

END()
