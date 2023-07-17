UNITTEST_FOR(cloud/blockstore/libs/storage/volume/actors)

SRCS(
    forward_read.ut.cpp
    forward_write_and_mark_used.ut.cpp
)

PEERDIR(
    library/cpp/actors/testlib
)

END()
