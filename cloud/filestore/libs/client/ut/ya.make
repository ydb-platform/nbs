UNITTEST_FOR(cloud/filestore/libs/client)

INCLUDE(${ARCADIA_ROOT}/cloud/filestore/tests/recipes/small.inc)

SRCS(
    client_ut.cpp
    durable_ut.cpp
    session_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/diagnostics
    cloud/filestore/libs/server
)

END()
