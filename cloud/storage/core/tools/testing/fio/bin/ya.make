PACKAGE()

IF(ARCH_X86_64)
    FROM_SANDBOX(
        11122334486
        EXECUTABLE
        RENAME usr/bin/fio
        OUT fio)
ELSEIF (ARCH_ARM64)
    FROM_SANDBOX(
        11122334400
        EXECUTABLE
        RENAME usr/bin/fio
        OUT fio)
ELSE()
    MESSAGE(FATAL_ERROR "Unsupported platform")
ENDIF()

END()
