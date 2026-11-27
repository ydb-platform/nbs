PACKAGE()

IF (ARCH_X86_64)
    # ya make -r -DFORCE_STATIC_LINKING=yes -DALLOCATOR=TCMALLOC_256K cloud/filestore/apps/vhost cloud/filestore/apps/client
    # tar -chzf "filestore-$(uname -m).tar.gz" -C cloud/filestore/apps/vhost filestore-vhost -C ../client filestore-client
    SET(VIRTIOFS_FILESTORE_RESOURCE 13346432341)
ELSEIF (ARCH_ARM64)
    # ya make -r -DFORCE_STATIC_LINKING=yes -DALLOCATOR=TCMALLOC_256K --target-platform=default-linux-aarch64  cloud/filestore/apps/vhost cloud/filestore/apps/client
    # tar -chzf filestore-aarch64.tar.gz -C cloud/filestore/apps/vhost filestore-vhost -C ../client filestore-client
    SET(VIRTIOFS_FILESTORE_RESOURCE 13346432342)
ELSE()
    MESSAGE(FATAL_ERROR "Unsupported platform")
ENDIF()

FROM_SANDBOX(
    ${VIRTIOFS_FILESTORE_RESOURCE}
    EXECUTABLE
    OUT filestore-vhost filestore-client
)

END()
