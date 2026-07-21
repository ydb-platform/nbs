LIBRARY()

SRCS(
    export.cpp
)

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_OPS
    )
ENDIF()

PEERDIR(
    contrib/ydb/library/yql/public/types
    contrib/ydb/core/tx/columnshard/engines/scheme/defaults/protos
    contrib/ydb/library/mkql_proto/protos
    contrib/ydb/library/aclib/protos
    contrib/ydb/library/formats/arrow/protos
    contrib/ydb/core/tx/datashard
)

END()
