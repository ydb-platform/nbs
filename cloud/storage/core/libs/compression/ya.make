LIBRARY()

SRCS(
    blob_compression.cpp
    codec.cpp
)

PEERDIR(
    cloud/storage/core/libs/common
    cloud/storage/core/protos

    library/cpp/blockcodecs
)

END()

RECURSE_FOR_TESTS(
    ut
)
