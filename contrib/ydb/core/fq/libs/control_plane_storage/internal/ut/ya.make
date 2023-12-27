UNITTEST_FOR(contrib/ydb/core/fq/libs/control_plane_storage/internal)

SIZE(MEDIUM)

SRCS(utils_ut.cpp)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/json/yson
    contrib/ydb/library/yql/public/udf/service/stub
)

YQL_LAST_ABI_VERSION()

END()

