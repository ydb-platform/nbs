UNITTEST_FOR(contrib/ydb/core/wrappers)

FORK_SUBTESTS()

SPLIT_FACTOR(20)

IF (NOT OS_WINDOWS)
    PEERDIR(
        contrib/ydb/library/actors/core
        library/cpp/digest/md5
        library/cpp/testing/unittest
        contrib/libs/aws-sdk-cpp/aws-cpp-sdk-core
        contrib/ydb/core/protos
        contrib/ydb/core/testlib/basics/default
        contrib/ydb/library/yql/minikql/comp_nodes/llvm14
        contrib/ydb/core/wrappers/ut_helpers
    )
    SRCS(
        s3_wrapper_ut.cpp
    )
ENDIF()

YQL_LAST_ABI_VERSION()

REQUIREMENTS(ram:12)

END()
