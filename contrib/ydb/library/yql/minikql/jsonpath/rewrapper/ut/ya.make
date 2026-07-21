UNITTEST_FOR(contrib/ydb/library/yql/minikql/jsonpath/rewrapper)

IF(ARCH_X86_64)
    SRCS(
        hyperscan_ut.cpp
        re2_ut.cpp
    )

    PEERDIR(
        contrib/ydb/library/yql/minikql/jsonpath/rewrapper
        contrib/ydb/library/yql/minikql/jsonpath/rewrapper/hyperscan
        contrib/ydb/library/yql/minikql/jsonpath/rewrapper/re2
    )
ELSE()
    SRCS(
        re2_ut.cpp
    )

    PEERDIR(
        contrib/ydb/library/yql/minikql/jsonpath/rewrapper
        contrib/ydb/library/yql/minikql/jsonpath/rewrapper/re2
    )
ENDIF()

END()
