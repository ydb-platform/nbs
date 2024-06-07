LIBRARY()

SRCS(
    tx_controller.cpp
)

PEERDIR(
    contrib/ydb/core/tablet_flat
    contrib/ydb/core/tx/data_events
)

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_OPS
    )
ENDIF()

YQL_LAST_ABI_VERSION()

END()
