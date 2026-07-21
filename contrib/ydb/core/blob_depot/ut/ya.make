UNITTEST_FOR(contrib/ydb/core/blob_depot)

    SIZE(MEDIUM)

    IF (NOT OS_WINDOWS)
        SRCS(
            s3_router_ut.cpp
        )

        PEERDIR(
            contrib/ydb/core/testlib/default
            contrib/ydb/library/actors/http
            contrib/ydb/library/aws_init
        )
    ENDIF()

    SRCS(
        closed_interval_set_ut.cpp
        given_id_range_ut.cpp
    )

END()
