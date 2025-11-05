UNITTEST_FOR(contrib/ydb/core/mind/address_classification)

FORK_SUBTESTS()

IF (SANITIZER_TYPE == "thread" OR WITH_VALGRIND)
    TIMEOUT(1200)
    SIZE(LARGE)
    SPLIT_FACTOR(20)
    TAG(ya:fat)
    REQUIREMENTS(ram:16)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
    REQUIREMENTS(ram:16)
ENDIF()

PEERDIR(
    contrib/ydb/library/actors/http
    contrib/ydb/core/mind/address_classification
    contrib/ydb/core/testlib/default
)

YQL_LAST_ABI_VERSION()

SRCS(
    net_classifier_ut.cpp
)

END()
