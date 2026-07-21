UNITTEST_FOR(contrib/ydb/library/yql/udfs/common/reservoir_sampling/lib)

FORK_SUBTESTS()
PEERDIR(contrib/ydb/library/yql/udfs/common/reservoir_sampling/lib)
SRCS(
    reservoir_ut.cpp
)

PEERDIR()

END()
