UNITTEST_FOR(cloud/storage/core/libs/actors)

SRCS(
    actor_pool_ut.cpp
    poison_pill_helper_ut.cpp
)

PEERDIR(
    contrib/ydb/library/actors/testlib

    contrib/ydb/core/testlib
    contrib/ydb/core/testlib/default
    contrib/ydb/core/testlib/basics

    library/cpp/testing/unittest
)

END()
