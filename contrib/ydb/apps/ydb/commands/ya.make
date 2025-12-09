LIBRARY(commands)

SRCS(
    ydb_root.cpp
    ydb_update.cpp
    ydb_version.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/src/client/iam
    contrib/ydb/public/lib/ydb_cli/commands
)

END()
