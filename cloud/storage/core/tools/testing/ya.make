RECURSE(
    access_service
    access_service_new
    fio
    pssh-mock
    qemu
    threadpool-test
    unstable-process
    virtiofs_server
    ydb
)

IF (OPENSOURCE)
    RECURSE(silk_demo)
ENDIF()
