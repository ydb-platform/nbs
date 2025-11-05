LIBRARY()

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow
    contrib/libs/apache/arrow
    contrib/ydb/library/actors/core
    contrib/ydb/core/tx/columnshard/blobs_action/bs
    contrib/ydb/core/tx/columnshard
    contrib/ydb/core/wrappers
)

SRCS(
    helper.cpp
    controllers.cpp
    columnshard_ut_common.cpp
)

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_OPS
    )
ELSE()
    PEERDIR(
        contrib/ydb/core/tx/columnshard/blobs_action/tier
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()

