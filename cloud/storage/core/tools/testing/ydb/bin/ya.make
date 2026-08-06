IF(USE_BUNDLED_YDBD)
    # This file contrib/ydb/apps/ydbd/ya.make should contain SRCDIR(contrib/ydb/apps/ydbd) inside PROGRAM()
    # It can be removed by code sync
    INCLUDE(${ARCADIA_ROOT}/contrib/ydb/apps/ydbd/ya.make)
ELSE()

PACKAGE()

IF(ARCH_X86_64)
    FROM_SANDBOX(
        12345567801
        AUTOUPDATED ydbd
        EXECUTABLE
        OUT ydbd
    )
ELSEIF (ARCH_ARM64)
    FROM_SANDBOX(
        12345567802
        AUTOUPDATED ydbd
        EXECUTABLE
        OUT ydbd
    )
ELSE()
    MESSAGE(FATAL_ERROR "Unsupported platform")
ENDIF()

END()

ENDIF()
