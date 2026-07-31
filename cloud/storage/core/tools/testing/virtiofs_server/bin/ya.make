PACKAGE()

IF(ARCH_X86_64)
    FROM_SANDBOX(
        FILE
        111111111241
        RENAME RESOURCE
        OUT_NOAUTO virtiofs-server
        EXECUTABLE)
ELSEIF (ARCH_ARM64)
    FROM_SANDBOX(
        FILE
        111111111242
        RENAME RESOURCE
        OUT_NOAUTO virtiofs-server
        EXECUTABLE)
ELSE()
    MESSAGE(FATAL_ERROR "Unsupported platform")
ENDIF()

END()
