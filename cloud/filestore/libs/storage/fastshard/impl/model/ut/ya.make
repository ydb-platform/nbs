GTEST()

SRCS(
    ../persistent_bitmap_ut.cpp
    ../persistent_hash_table_ut.cpp
)

PEERDIR(
    cloud/filestore/libs/storage/fastshard/impl/model

    cloud/storage/core/libs/common
    cloud/storage/core/protos

    contrib/restricted/googletest/googletest
)

END()
