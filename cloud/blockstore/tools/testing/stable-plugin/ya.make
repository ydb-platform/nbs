PACKAGE()

IF(ARCH_X86_64)
    # ya make -r -DALLOCATOR=TCMALLOC_256K cloud/vm/blockstore
    # tar -czvhf libblockstore-plugin-amd64.tgz libblockstore-plugin.so
    FROM_SANDBOX(13124593141 OUT_NOAUTO libblockstore-plugin.so)
ELSEIF (ARCH_ARM64)
    # ya make -r -DALLOCATOR=TCMALLOC_256K --target-platform=default-linux-aarch64 cloud/vm/blockstore
    # tar -czvhf libblockstore-plugin-arm64.tgz libblockstore-plugin.so
    FROM_SANDBOX(13124596655 OUT_NOAUTO libblockstore-plugin.so)
ELSE()
    MESSAGE(FATAL_ERROR "Unsupported platform")
ENDIF()

END()
