RECURSE(
    access_service
    access_service_new
    codec_bench
    fio
    pssh-mock
    qemu
    rdma-rnr-repro
    threadpool-test
    unstable-process
    virtiofs_server
    ydb
)

IF (OPENSOURCE)
    RECURSE(silk_demo)
ENDIF()
