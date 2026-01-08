UNITTEST_FOR(contrib/ydb/core/mind/hive)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    library/cpp/getopt
    library/cpp/svnversion
    contrib/ydb/library/actors/helpers
    contrib/ydb/core/base
    contrib/ydb/core/mind
    contrib/ydb/core/mind/hive
    contrib/ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    object_distribution_ut.cpp
    scale_recommender_policy_ut.cpp
    sequencer_ut.cpp
    storage_pool_info_ut.cpp
    hive_ut.cpp
    hive_impl_ut.cpp
)

END()
