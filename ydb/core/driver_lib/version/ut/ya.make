UNITTEST_FOR(ydb/core/driver_lib/version)

SRCS(version_ut.cpp)

TIMEOUT(300)
SIZE(MEDIUM)

PEERDIR(
    ydb/apps/version
    ydb/core/driver_lib/version
)

END()
