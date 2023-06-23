IF (OPENSOURCE)
    RESTRICT_LICENSES(
        DENY REQUIRE_DISCLOSURE FORBIDDEN PROTESTWARE
        EXCEPT contrib/libs/linux-headers   # DTCC-553
        # shared with ydb - if they get rid of these libs, we will also lose these dependencies
        EXCEPT contrib/libs/fmt             #
        EXCEPT contrib/libs/t1ha            # KIKIMR-16412
    )
ENDIF()
