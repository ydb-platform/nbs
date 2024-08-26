LIBRARY()

SRCS(
    topic_api.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/logbroker/iface

    contrib/ydb/public/sdk/cpp/client/iam
    contrib/ydb/public/sdk/cpp/client/ydb_driver
    contrib/ydb/public/sdk/cpp/client/ydb_topic

    library/cpp/threading/future
)

END()

# see https://github.com/ydb-platform/ydb/issues/8170
IF (SANITIZER_TYPE != "undefined")

RECURSE_FOR_TESTS(ut)

ENDIF()
