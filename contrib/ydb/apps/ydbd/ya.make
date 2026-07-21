PROGRAM(ydbd)

IF (NOT SANITIZER_TYPE)  # for some reasons some tests with asan are failed, see comment in CPPCOM-32
    # Disabling export of dynamic symbols allows to significantly reduce size of the stripped binary,
    # however, to be able to use dynamic UDFs (the --udfs-dir flag of ydbd server),
    # required explicit export of symbols from contrib/ydb/library/yql/public/udf/service/exception_policy/udf_service.cpp
    IF (OS_LINUX)
        EXPORTS_SCRIPT(contrib/ydb/apps/ydbd/exports.symlist)
    ELSE()
        NO_EXPORT_DYNAMIC_SYMBOLS()
    ENDIF()
ENDIF()

IF (OS_LINUX)
    ALLOCATOR(TCMALLOC_256K)
    LINKER_SCRIPT(strip_debug.ld)
ENDIF()

IF (OS_DARWIN)
    STRIP()
    NO_SPLIT_DWARF()
ENDIF()

IF (OS_WINDOWS)
    CFLAGS(
        -DKIKIMR_DISABLE_S3_OPS
    )
ENDIF()

SRCS(
    main.cpp
)

IF (ARCH_X86_64)
    PEERDIR(
        contrib/ydb/library/yql/udfs/common/hyperscan
    )
ENDIF()

PEERDIR(
    contrib/ydb/apps/version
    contrib/ydb/apps/ydbd/export
    contrib/ydb/core/driver_lib/run
    contrib/ydb/core/protos
    contrib/ydb/core/security
    contrib/ydb/core/tx/schemeshard
    contrib/ydb/core/ymq/actor
    contrib/ydb/core/ymq/base
    contrib/ydb/library/breakpad
    contrib/ydb/library/folder_service/mock
    contrib/ydb/library/keys
    contrib/ydb/library/pdisk_io
    contrib/ydb/library/security
    contrib/ydb/library/yql/udfs/common/clickhouse/client
    contrib/ydb/library/yql/udfs/common/hybrid_search
    contrib/ydb/library/yql/udfs/common/knn
    contrib/ydb/library/yql/udfs/common/roaring
    contrib/ydb/library/yql/udfs/statistics_internal
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/udfs/common/compress_base
    contrib/ydb/library/yql/udfs/common/datetime2
    contrib/ydb/library/yql/udfs/common/digest
    contrib/ydb/library/yql/udfs/common/histogram
    contrib/ydb/library/yql/udfs/common/hyperloglog
    contrib/ydb/library/yql/udfs/common/ip_base
    contrib/ydb/library/yql/udfs/common/json
    contrib/ydb/library/yql/udfs/common/json2
    contrib/ydb/library/yql/udfs/common/math
    contrib/ydb/library/yql/udfs/common/pire
    contrib/ydb/library/yql/udfs/common/re2
    contrib/ydb/library/yql/udfs/common/set
    contrib/ydb/library/yql/udfs/common/stat
    contrib/ydb/library/yql/udfs/common/string
    contrib/ydb/library/yql/udfs/common/top
    contrib/ydb/library/yql/udfs/common/topfreq
    contrib/ydb/library/yql/udfs/common/unicode_base
    contrib/ydb/library/yql/udfs/common/url_base
    contrib/ydb/library/yql/udfs/common/yson2
    contrib/ydb/library/yql/udfs/logs/dsv
)

YQL_LAST_ABI_VERSION()

END()
