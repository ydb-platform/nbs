PROGRAM(ydbd)

IF (NOT SANITIZER_TYPE)  # for some reasons some tests with asan are failed, see comment in CPPCOM-32
    NO_EXPORT_DYNAMIC_SYMBOLS()
ENDIF()

IF (OS_LINUX)
    ALLOCATOR(TCMALLOC_256K)
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

SRCDIR(
     contrib/ydb/apps/ydbd
)

SRCS(
    export.cpp
    export.h
    main.cpp
)

IF (ARCH_X86_64)
    PEERDIR(
        contrib/ydb/library/yql/udfs/common/hyperscan
    )
ENDIF()

PEERDIR(
    contrib/ydb/apps/version
    contrib/ydb/core/driver_lib/run
    contrib/ydb/core/protos
    contrib/ydb/core/security
    contrib/ydb/core/ymq/actor
    contrib/ydb/core/ymq/base
    contrib/ydb/library/folder_service/mock
    contrib/ydb/library/keys
    contrib/ydb/library/pdisk_io
    contrib/ydb/library/security
    contrib/ydb/library/yql/parser/pg_wrapper
    contrib/ydb/library/yql/sql/pg
    contrib/ydb/library/yql/udfs/common/clickhouse/client
    contrib/ydb/library/yql/udfs/common/compress_base
    contrib/ydb/library/yql/udfs/common/datetime
    contrib/ydb/library/yql/udfs/common/datetime2
    contrib/ydb/library/yql/udfs/common/digest
    contrib/ydb/library/yql/udfs/common/histogram
    contrib/ydb/library/yql/udfs/common/hyperloglog
    contrib/ydb/library/yql/udfs/common/ip_base
    contrib/ydb/library/yql/udfs/common/knn
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
    contrib/ydb/public/sdk/cpp/client/ydb_persqueue_public/codecs
)

#
# DON'T ALLOW NEW DEPENDENCIES WITHOUT EXPLICIT APPROVE FROM  kikimr-dev@ or fomichev@
#
CHECK_DEPENDENT_DIRS(
    ALLOW_ONLY
    PEERDIRS
    build
    certs
    contrib
    library
    tools/archiver
    tools/enum_parser/enum_parser
    tools/enum_parser/enum_serialization_runtime
    tools/rescompressor
    tools/rorescompiler
    util
    contrib/ydb
    yt
)

YQL_LAST_ABI_VERSION()

END()

