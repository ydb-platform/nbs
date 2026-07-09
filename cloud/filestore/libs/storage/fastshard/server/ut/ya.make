GTEST()

SRCS(
    ../server_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/client
    cloud/filestore/libs/storage/fastshard/server
    cloud/filestore/libs/storage/fastshard/server/protos
    cloud/filestore/libs/storage/fastshard/impl/mem
    cloud/filestore/libs/storage/fastshard/testlib

    cloud/filestore/private/api/unsafe_protos

    library/cpp/testing/common

    contrib/libs/silk/src/fibers

    contrib/restricted/googletest/googletest
)

END()
