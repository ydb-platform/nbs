IF (NOT SANITIZER_TYPE)

UNITTEST()

TAG(ya:manual)

PEERDIR(
    library/cpp/protobuf/util
    contrib/ydb/library/yql/public/udf/service/exception_policy
    contrib/ydb/library/yql/public/purecalc
    contrib/ydb/library/yql/public/purecalc/io_specs/protobuf
    contrib/ydb/library/yql/public/purecalc/ut/protos
)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

SRCS(
    test_spec.cpp
)

END()

ENDIF()
