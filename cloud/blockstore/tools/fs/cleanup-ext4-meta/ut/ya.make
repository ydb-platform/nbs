UNITTEST_FOR(cloud/blockstore/tools/fs/cleanup-ext4-meta)

SRCS(
    ext4-meta-reader_ut.cpp
    ext4-meta-reader.cpp
)

PEERDIR(
    cloud/storage/core/libs/common

    library/cpp/regex/pcre
)

DATA(ext:empty-ext4.txt)

END()
