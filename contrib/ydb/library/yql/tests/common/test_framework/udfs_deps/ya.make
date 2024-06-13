SET(
    UDFS
    contrib/ydb/library/yql/udfs/common/datetime2
    contrib/ydb/library/yql/udfs/common/digest
    contrib/ydb/library/yql/udfs/common/file
    contrib/ydb/library/yql/udfs/common/hyperloglog
    contrib/ydb/library/yql/udfs/common/pire
    contrib/ydb/library/yql/udfs/common/protobuf
    contrib/ydb/library/yql/udfs/common/re2
    contrib/ydb/library/yql/udfs/common/set
    contrib/ydb/library/yql/udfs/common/stat
    contrib/ydb/library/yql/udfs/common/topfreq
    contrib/ydb/library/yql/udfs/common/top
    contrib/ydb/library/yql/udfs/common/string
    contrib/ydb/library/yql/udfs/common/histogram
    contrib/ydb/library/yql/udfs/common/json2
    contrib/ydb/library/yql/udfs/common/yson2
    contrib/ydb/library/yql/udfs/common/math
    contrib/ydb/library/yql/udfs/common/url_base
    contrib/ydb/library/yql/udfs/common/unicode_base
    contrib/ydb/library/yql/udfs/common/streaming
    contrib/ydb/library/yql/udfs/examples/callables
    contrib/ydb/library/yql/udfs/examples/dicts
    contrib/ydb/library/yql/udfs/examples/dummylog
    contrib/ydb/library/yql/udfs/examples/lists
    contrib/ydb/library/yql/udfs/examples/structs
    contrib/ydb/library/yql/udfs/examples/type_inspection
    contrib/ydb/library/yql/udfs/logs/dsv
    contrib/ydb/library/yql/udfs/test/simple
    contrib/ydb/library/yql/udfs/test/test_import
)

IF (OS_LINUX AND CLANG)
    SET(
        UDFS
        ${UDFS}
        contrib/ydb/library/yql/udfs/common/hyperscan
    )
ENDIF()

PACKAGE()

IF (SANITIZER_TYPE != "undefined")

PEERDIR(
    ${UDFS}
)

ENDIF()

END()
