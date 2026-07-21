LIBRARY()

SRCS(
    tvm_client_dummy.cpp
)

PEERDIR(
    contrib/ydb/library/yql/providers/yt/lib/tvm_client/dummy
    contrib/ydb/library/yql/providers/yt/lib/tvm_client/proto
)

IF (NOT OPENSOURCE)
    INCLUDE(ya_non_opensource.inc)
ENDIF()

END()
