LIBRARY()

SRCS(
    objects.h
    objects.cpp
    converter.h
    converter.cpp
)

PEERDIR(
    contrib/libs/protobuf
    contrib/ydb/core/protos
    contrib/ydb/core/scheme
    contrib/ydb/library/mkql_proto/protos
    contrib/ydb/public/lib/deprecated/kicli
)

END()

RECURSE_FOR_TESTS(
    ut
)
