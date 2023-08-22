UNITTEST_FOR(cloud/blockstore/libs/storage/volume/actors)

INCLUDE(${ARCADIA_ROOT}/cloud/blockstore/tests/recipes/small.inc)

SRCS(
    forward_read_marked.ut.cpp
    forward_write_and_mark_used.ut.cpp
)

PEERDIR(
    library/cpp/actors/testlib
)

END()
