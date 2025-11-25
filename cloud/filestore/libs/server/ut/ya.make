UNITTEST_FOR(cloud/filestore/libs/server)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCS(
    server_memory_state_ut.cpp
    server_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/client
    cloud/filestore/libs/diagnostics
)

DATA(
    arcadia/cloud/filestore/tests/certs/server.crt
    arcadia/cloud/filestore/tests/certs/server.key
    arcadia/cloud/filestore/tests/certs/server_fallback.crt
)

END()
