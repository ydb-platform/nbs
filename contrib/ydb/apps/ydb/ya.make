PROGRAM(ydb)

STRIP()

SRCS(
    main.cpp
)

PEERDIR(
    contrib/ydb/apps/ydb/commands
)

RESOURCE(
    contrib/ydb/apps/ydb/version.txt version.txt
)

IF (NOT USE_SSE4 AND NOT OPENSOURCE)
    # contrib/libs/glibasm can not be built without SSE4
    # Replace it with contrib/libs/asmlib which can be built this way.
    DISABLE(USE_ASMLIB)
    PEERDIR(
        contrib/libs/asmlib
    )
ENDIF()

#
# DON'T ALLOW NEW DEPENDENCIES WITHOUT EXPLICIT APPROVE FROM  kikimr-dev@ or fomichev@
#
CHECK_DEPENDENT_DIRS(
    ALLOW_ONLY
    PEERDIRS
    build/internal/platform
    build/platform
    certs
    contrib
    library
    tools/enum_parser/enum_parser
    tools/enum_parser/enum_serialization_runtime
    tools/rescompressor
    tools/rorescompiler
    util
    contrib/ydb/apps/ydb
    contrib/ydb/core/fq/libs/protos
    contrib/ydb/core/grpc_services/validation
    contrib/ydb/library
    contrib/ydb/public
    contrib/ydb/library/yql/public/decimal
    contrib/ydb/library/yql/public/issue
    contrib/ydb/library/yql/public/issue/protos
)

END()

IF (OS_LINUX)
    RECURSE_FOR_TESTS(
        ut
    )
ENDIF()
