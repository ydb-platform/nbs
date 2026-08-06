PACKAGE()

IF(ARCH_X86_64)
    FROM_SANDBOX(
        12978105235
        EXECUTABLE
        RENAME usr/bin/fio
        OUT fio)
ELSEIF (ARCH_ARM64)
    FROM_SANDBOX(
        12978105361
        EXECUTABLE
        RENAME usr/bin/fio
        OUT fio)
ELSE()
    MESSAGE(FATAL_ERROR "Unsupported platform")
ENDIF()

END()
