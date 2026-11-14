PY3_PROGRAM(virtiofs-server)

PY_SRCS(
    __main__.py
)

PEERDIR(
    cloud/storage/core/tools/testing/virtiofs_server/lib
)

DEPENDS(
    cloud/filestore/apps/client
    cloud/filestore/apps/vhost
)

END()
