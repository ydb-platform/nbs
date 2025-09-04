UNITTEST_FOR(contrib/ydb/core/driver_lib/version)

SRCS(version_ut.cpp)

SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/core/driver_lib/version
    contrib/ydb/apps/version
)

END()
