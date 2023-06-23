LIBRARY()

SRCS(
    blob_id.cpp
    gc_logic.cpp
)

PEERDIR(
    cloud/storage/core/libs/tablet/model
    library/cpp/actors/core
    ydb/core/base
)

END()

RECURSE(
    model
)

RECURSE_FOR_TESTS(
    ut
)
