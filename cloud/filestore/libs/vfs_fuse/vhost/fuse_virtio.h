#pragma once

#include <cloud/filestore/libs/vfs_fuse/fuse.h>

#include <cloud/contrib/vhost/include/vhost/fs.h>
#include <cloud/contrib/vhost/include/vhost/server.h>
#include <cloud/contrib/vhost/include/vhost/types.h>
#include <cloud/contrib/vhost/logging.h>
#include <cloud/contrib/vhost/platform.h>

#include <contrib/libs/virtiofsd/fuse.h>
#include <contrib/libs/virtiofsd/fuse_i.h>
#include <contrib/libs/virtiofsd/fuse_lowlevel.h>
#include <contrib/libs/virtiofsd/fuse_virtio.h>

struct fuse_virtio_dev
{
    struct vhd_fsdev_info fsdev;

    struct vhd_vdev* vdev;
    struct vhd_request_queue* rq;
};

struct fuse_virtio_queue
{
    // not used
};

struct fuse_virtio_request
{
    struct fuse_chan ch;
    struct vhd_io* io;

    void* buffer;
    bool response_sent;

    struct {
        struct iovec* iov;
        size_t count;
    } in;

    struct {
        struct iovec* iov;
        size_t count;
    } out;

    struct iovec iov[1];
};
#ifdef __cplusplus
extern "C" {
#endif

int process_request(struct fuse_session* se, struct vhd_io* io);

#ifdef __cplusplus
}
#endif

int virtio_session_loop(struct fuse_session* se)
{
    struct fuse_virtio_dev* dev = se->virtio_dev;

    int res;
    for (;;) {
        res = vhd_run_queue(dev->rq);
        if (res != -EAGAIN) {
            if (res < 0) {
                VHD_LOG_WARN("request queue failure %d", -res);
            }
            break;
        }

        struct vhd_request req;
        while (vhd_dequeue_request(dev->rq, &req)) {
            struct vhd_rq_metrics metrics_rq;
            vhd_get_rq_stat(dev->rq, &metrics_rq);

            struct vhd_vq_metrics metrics_vq;
            vhd_vdev_get_queue_stat(dev->vdev, 0, &metrics_vq);

            res = process_request(se, req.io);
            if (res < 0) {
                VHD_LOG_WARN("request processing failure %d", -res);
            }
        }
    }

    se->exited = 1;
    return res;
}
