FUZZ()

OWNER(g:cloud-nbs)

SIZE(LARGE)

TAG(
    ya:fat
    ya:not_autocheck
    ya:manual
)

SRCS(
    main.cpp
)

PEERDIR(
    cloud/blockstore/libs/daemon/ydb
    cloud/blockstore/libs/logbroker/iface
    cloud/blockstore/libs/service
    cloud/blockstore/tests/fuzzing/common

    cloud/storage/core/libs/iam/iface

    contrib/ydb/core/security

    library/cpp/testing/common
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(config)
