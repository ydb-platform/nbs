RECURSE(
    fio
    pssh-mock
    qemu
    threadpool-test
    unstable-process
)

IF (NOT OPENSOURCE)
    RECURSE(
        access_service      # NBS-4409
        virtiofs_server     # NBS-4409
    )
ENDIF()
