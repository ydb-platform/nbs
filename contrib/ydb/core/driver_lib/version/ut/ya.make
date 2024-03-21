UNITTEST_FOR(contrib/ydb/core/driver_lib/version)

SRCS(version_ut.cpp)

TIMEOUT(300)
SIZE(MEDIUM)

PEERDIR(
    contrib/ydb/apps/version
    contrib/ydb/core/driver_lib/version
)

END()
