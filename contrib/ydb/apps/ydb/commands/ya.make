LIBRARY(commands)

SRCS(
    ydb_cloud_root.cpp
    ydb_update.cpp
    ydb_version.cpp
)

PEERDIR(
    contrib/ydb/public/sdk/cpp/client/iam/common
    contrib/ydb/public/lib/ydb_cli/commands
)

END()
