IF (USE_PREBUILT_YDBD)
    DEPENDS(
        cloud/storage/core/tools/testing/ydb/bin
    )
    ENV(YDBD_BINARY="cloud/storage/core/tools/testing/ydb/bin/ydbd")
ELSE()
    DEPENDS(
        contrib/ydb/apps/ydbd
    )
    ENV(YDBD_BINARY="contrib/ydb/apps/ydbd/ydbd")
ENDIF()
