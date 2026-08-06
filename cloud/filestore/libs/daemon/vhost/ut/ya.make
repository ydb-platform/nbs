UNITTEST_FOR(cloud/filestore/libs/daemon/vhost)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCS(
    bootstrap_ut.cpp
    config_initializer_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/testlib

    contrib/ydb/core/testlib
)

DATA(
    arcadia/cloud/filestore/tests/certs/server.crt
    arcadia/cloud/filestore/tests/certs/server.key
)

YQL_LAST_ABI_VERSION()

END()
