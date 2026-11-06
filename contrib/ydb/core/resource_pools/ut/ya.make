UNITTEST_FOR(contrib/ydb/core/resource_pools)

PEERDIR(
    library/cpp/testing/unittest

    contrib/ydb/core/resource_pools
    yql/essentials/sql/v1_dummy
)

SRCS(
    resource_pool_classifier_settings_ut.cpp
    resource_pool_settings_ut.cpp
)

END()
