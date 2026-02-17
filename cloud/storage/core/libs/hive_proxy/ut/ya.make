UNITTEST_FOR(cloud/storage/core/libs/hive_proxy)

cloud/storage/core/tests/recipes/medium.inc

SRCS(
    hive_proxy_ut.cpp
)

PEERDIR(
    contrib/ydb/core/testlib
    contrib/ydb/core/testlib/default
    contrib/ydb/core/testlib/basics
)

END()
