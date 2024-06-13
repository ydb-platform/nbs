IF (NOT SANITIZER_TYPE AND NOT OPENSOURCE)

EXECTEST()

RUN(protobuf ${ARCADIA_BUILD_ROOT}/contrib/ydb/library/yql/udfs STDOUT log.out CANONIZE_LOCALLY log.out)

DEPENDS(
    contrib/ydb/library/yql/public/purecalc/examples/protobuf
    contrib/ydb/library/yql/udfs/common/url_base
    contrib/ydb/library/yql/udfs/common/ip_base
)

END()

ENDIF()
