PACKAGE()

IF(ARCH_X86_64)
    FROM_SANDBOX(
        FILE
        4449551218
        AUTOUPDATED qemu_binary
        RENAME RESOURCE
        OUT_NOAUTO qemu-bin.tar.gz
    )
ELSEIF (ARCH_ARM64)
    FROM_SANDBOX(
        FILE
        14444444444
        AUTOUPDATED qemu_binary
        RENAME RESOURCE
        OUT_NOAUTO qemu-bin.tar.gz
    )
ELSE()
    MESSAGE(FATAL_ERROR "Unsupported platform")
ENDIF()

END()
