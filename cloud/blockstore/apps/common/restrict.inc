IF (OPENSOURCE)
    LICENSE_RESTRICTION(
        DENY REQUIRE_DISCLOSURE FORBIDDEN PROTESTWARE
    )
    LICENSE_RESTRICTION_EXCEPTIONS(
        contrib/libs/linux-headers   # DTCC-553
        # shared with ydb - if they get rid of these libs, we will also lose these dependencies
        contrib/libs/fmt             #
        contrib/libs/t1ha            # KIKIMR-16412
    )
ENDIF()
