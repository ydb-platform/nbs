UNITTEST_FOR(contrib/ydb/library/actors/interconnect/v2)

IF (SANITIZER_TYPE == "thread")
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    v2_chunk_serializer_ut.cpp
    v2_event_serializer_ut.cpp
    v2_io_buffers_ut.cpp
    v2_serialize_window_ut.cpp
)

PEERDIR(
    contrib/ydb/library/actors/core
    contrib/ydb/library/actors/protos
    library/cpp/testing/unittest
)

END()
