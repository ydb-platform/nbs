UNITTEST_FOR(cloud/blockstore/libs/vhost)

SIZE(MEDIUM)
TIMEOUT(600)
REQUIREMENTS(ram:32)

SRCS(
    server_ut_stress.cpp
)

END()
