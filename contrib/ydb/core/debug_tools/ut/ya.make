UNITTEST()

    TIMEOUT(60)
    SIZE(SMALL)

    PEERDIR(
        contrib/ydb/core/debug_tools
    )

    SRCS(
        main.cpp
    )
    
END()
