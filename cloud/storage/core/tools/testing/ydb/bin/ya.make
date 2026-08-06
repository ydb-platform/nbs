IF(USE_BUNDLED_YDBD)
    # This file contrib/ydb/apps/ydbd/ya.make should contain SRCDIR(contrib/ydb/apps/ydbd) inside PROGRAM()
    # It can be removed by code sync
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/apps/ydbd/ya.make)
ELSE()
    PACKAGE()

    FROM_SANDBOX(
        11058457307
        AUTOUPDATED ydbd
        EXECUTABLE
        OUT ydbd
    )

    END()
ENDIF()
