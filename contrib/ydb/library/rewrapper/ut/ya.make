UNITTEST_FOR(contrib/ydb/library/rewrapper)

IF(ARCH_X86_64)
    SRCS(
        hyperscan_ut.cpp
        re2_ut.cpp
    )

    PEERDIR(
        contrib/ydb/library/rewrapper
        contrib/ydb/library/rewrapper/hyperscan
        contrib/ydb/library/rewrapper/re2
    )
ELSE()
    SRCS(
        re2_ut.cpp
    )

    PEERDIR(
        contrib/ydb/library/rewrapper
        contrib/ydb/library/rewrapper/re2
    )
ENDIF()

END()
