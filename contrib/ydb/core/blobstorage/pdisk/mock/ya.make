LIBRARY()

SRCS(
    pdisk_mock.cpp
    pdisk_mock.h
)

PEERDIR(
    library/cpp/actors/core
    contrib/ydb/core/blobstorage/pdisk
)

END()
