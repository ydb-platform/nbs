LIBRARY()

SRCS(
    topic_api.cpp
)

PEERDIR(
    cloud/blockstore/libs/common
    cloud/blockstore/libs/diagnostics
    cloud/blockstore/libs/logbroker/iface

    contrib/ydb/public/sdk/cpp/src/client/iam
    contrib/ydb/public/sdk/cpp/src/client/driver
    contrib/ydb/public/sdk/cpp/src/client/topic

    library/cpp/threading/future
)

END()

# see https://github.com/ydb-platform/ydb/issues/8170
# and https://github.com/ydb-platform/ydb/issues/10992
IF (SANITIZER_TYPE != "undefined" AND SANITIZER_TYPE != "thread")

RECURSE_FOR_TESTS(ut)

ENDIF()
