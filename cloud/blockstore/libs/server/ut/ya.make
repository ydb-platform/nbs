UNITTEST_FOR(cloud/blockstore/libs/server)

IF (WITH_VALGRIND)
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    server_ut.cpp
)

PEERDIR(
    cloud/blockstore/libs/client
)

DATA(
    arcadia/cloud/blockstore/tests/certs/server.crt
    arcadia/cloud/blockstore/tests/certs/server.key
    arcadia/cloud/blockstore/tests/certs/server_fallback.crt
)

END()
