LIBRARY()

SRCS(
    write.cpp
    write_data.cpp
    slice_builder.cpp
)

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/tx/ev_write
    contrib/ydb/services/metadata
)

END()
