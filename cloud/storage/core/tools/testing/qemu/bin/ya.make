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
    # cloud/storage/core/tools/testing/qemu/build/__main__.py --co --git-tag v11.0.2 --git https://github.com/qemu/qemu --deps
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
