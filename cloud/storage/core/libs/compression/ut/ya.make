UNITTEST_FOR(cloud/storage/core/libs/compression)

INCLUDE(${ARCADIA_ROOT}/cloud/storage/core/tests/recipes/small.inc)

SRCS(
    blob_compression_ut.cpp
    codec_ut.cpp
)

END()
