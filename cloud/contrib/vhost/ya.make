LIBRARY(vhost-server)

CFLAGS(
    -Wno-unused-parameter
)

SRCS(
    blockdev.c
    event.c
    fs.c
    logging.c
    memlog.c
    memmap.c
    server.c
    vdev.c
    virtio/virt_queue.c
    virtio/virtio_blk.c
    virtio/virtio_fs.c
)

ADDINCL(
    GLOBAL cloud/contrib/vhost/include
    cloud/contrib/vhost
)

END()
