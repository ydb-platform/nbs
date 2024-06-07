LIBRARY()

SRCS(
    limits.h
    reverse_accessor.cpp
    scalars.cpp
    snapshot.cpp
    portion.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/libs/apache/arrow
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/common/protos
)

GENERATE_ENUM_SERIALIZATION(portion.h)

END()
