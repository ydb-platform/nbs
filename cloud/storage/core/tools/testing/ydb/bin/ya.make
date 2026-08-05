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
