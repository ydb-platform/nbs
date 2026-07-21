LIBRARY()

PEERDIR(
    contrib/ydb/core/protos
    contrib/ydb/core/formats/arrow
    contrib/ydb/core/tx/columnshard/engines/protos
    contrib/libs/apache/arrow
    contrib/ydb/library/actors/core
    contrib/ydb/core/tx/columnshard/blobs_action/bs
    contrib/ydb/library/formats/arrow/protos
    contrib/ydb/library/yql/minikql
    contrib/ydb/library/yql/minikql/invoke_builtins
    contrib/ydb/library/yql/core/arrow_kernels/request
    contrib/ydb/core/tx/columnshard
    contrib/ydb/core/tx/long_tx_service/public
    contrib/ydb/core/wrappers
    contrib/ydb/public/lib/value
)

SRCS(
    helper.cpp
    controllers.cpp
    columnshard_ut_common.cpp
    shard_reader.cpp
    shard_writer.cpp
    kernels_wrapper.cpp
    program_constructor.cpp
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

