RECURSE(
    qemu
)

IF (NOT OPENSOURCE)
    RECURSE(
        access-service      # NBS-4409
        virtiofs-server     # NBS-4409
    )
ENDIF()
