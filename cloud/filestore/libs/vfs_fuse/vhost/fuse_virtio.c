#include "fuse_virtio.h"

#include <cloud/filestore/libs/vfs_fuse/fuse.h>

#include <cloud/contrib/vhost/include/vhost/fs.h>
#include <cloud/contrib/vhost/include/vhost/server.h>
#include <cloud/contrib/vhost/logging.h>
#include <cloud/contrib/vhost/platform.h>

#include <contrib/libs/virtiofsd/fuse.h>
#include <contrib/libs/virtiofsd/fuse_i.h>
#include <contrib/libs/virtiofsd/fuse_lowlevel.h>
#include <contrib/libs/virtiofsd/fuse_virtio.h>

#include <stdatomic.h>
#include <sys/stat.h>

////////////////////////////////////////////////////////////////////////////////

#ifndef Y_UNUSED
#define Y_UNUSED(x) (void)x;
#endif

struct fuse_virtio_dev
{
    struct vhd_fsdev_info fsdev;

    struct vhd_vdev* vdev;
    struct vhd_request_queue** rqs;
    int rq_count;
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

struct unregister_context {
    struct fuse_session* se;
    atomic_bool completed;
};

#define VIRTIO_REQ_FROM_CHAN(ch) containerof(ch, struct fuse_virtio_request, ch);

////////////////////////////////////////////////////////////////////////////////

static size_t iov_size(struct iovec* iov, size_t count)
{
    size_t len = 0;
    for (size_t i = 0; i < count; ++i) {
        len += iov[i].iov_len;
    }

    return len;
}

static void iov_copy_to_iov(
    struct iovec* dst_iov, size_t dst_count,
    struct iovec* src_iov, size_t src_count,
    size_t to_copy)
{
    size_t dst_offset = 0;
    /* Outer loop copies 'src' elements */
    while (to_copy) {
        VHD_ASSERT(src_count);
        Y_UNUSED(src_count);
        size_t src_len = src_iov[0].iov_len;
        size_t src_offset = 0;

        if (src_len > to_copy) {
            src_len = to_copy;
        }
        /* Inner loop copies contents of one 'src' to maybe multiple dst. */
        while (src_len) {
            VHD_ASSERT(dst_count);
            Y_UNUSED(dst_count);
            size_t dst_len = dst_iov[0].iov_len - dst_offset;
            if (dst_len > src_len) {
                dst_len = src_len;
            }

            // iov with NULL iov_base can be placed if data was already copied
            // to dst buffers. This is done when zero copy is enabled for read
            // requests
            if (src_iov[0].iov_base) {
                memcpy(dst_iov[0].iov_base + dst_offset,
                       src_iov[0].iov_base + src_offset, dst_len);
            }
            src_len -= dst_len;
            to_copy -= dst_len;
            src_offset += dst_len;
            dst_offset += dst_len;

            VHD_ASSERT(dst_offset <= dst_iov[0].iov_len);
            if (dst_offset == dst_iov[0].iov_len) {
                dst_offset = 0;
                dst_iov++;
                dst_count--;
            }
        }
        src_iov++;
        src_count--;
    }
}

static void split_request_buffers(
    struct fuse_virtio_request* req,
    struct vhd_sglist* sglist)
{
    size_t i = 0;

    req->in.iov = req->iov;
    for (; i < sglist->nbuffers; ++i) {
        struct vhd_buffer* buf = &sglist->buffers[i];
        if (buf->write_only) {
            break;
        }
        struct iovec* iov = &req->in.iov[req->in.count++];
        iov->iov_base = buf->base;
        iov->iov_len = buf->len;
    }

    req->out.iov = &req->iov[i];
    for (; i < sglist->nbuffers; ++i) {
        struct vhd_buffer* buf = &sglist->buffers[i];
        if (!buf->write_only) {
            break;
        }
        struct iovec* iov = &req->out.iov[req->out.count++];
        iov->iov_base = buf->base;
        iov->iov_len = buf->len;
    }
}

static void complete_request(struct fuse_virtio_request* req, int res)
{
    VHD_ASSERT(!req->response_sent);
    req->response_sent = true;

    vhd_complete_bio(req->io, res);

    vhd_free(req->buffer);
    vhd_free(req);
}

static bool is_write_request(struct fuse_in_header* in)
{
    return in->opcode == FUSE_WRITE;
}

static bool is_ignored_request(struct fuse_in_header* in)
{
    return in->opcode == FUSE_INTERRUPT;
}

struct iov_iter {
    struct iovec *iov;
    size_t count;
    size_t idx;
    size_t off;
};

static size_t iov_iter_to_buf(struct iov_iter *it, void *buf, size_t len)
{
    void *ptr = buf;
    while (it->idx < it->count && len) {
        struct iovec *iov = &it->iov[it->idx];
        size_t cplen = MIN(len, iov->iov_len - it->off);
        memcpy(ptr, iov->iov_base + it->off, cplen);
        ptr += cplen;
        len -= cplen;
        it->off += cplen;
        if (it->off == iov->iov_len) {
            it->idx++;
            it->off = 0;
        }
    }
    return ptr - buf;
}

static int process_request(struct fuse_session* se, struct vhd_io* io)
{
    struct vhd_fs_io* fsio = vhd_get_fs_io(io);
    VHD_ASSERT(fsio->sglist.nbuffers > 0);

    struct fuse_virtio_request* req = vhd_zalloc(
        sizeof(struct fuse_virtio_request) +
        sizeof(struct iovec) * (fsio->sglist.nbuffers - 1));

    req->io = io;

    split_request_buffers(req, &fsio->sglist);
    VHD_ASSERT(req->in.count + req->out.count == fsio->sglist.nbuffers);

    size_t len = iov_size(req->in.iov, req->in.count);

    VHD_LOG_DEBUG("request with %zu IN desc of length %zu "
                  "and %zu OUT desc of length %zu\n",
        req->in.count, len,
        req->out.count, iov_size(req->out.iov, req->out.count));

    if (len > se->bufsize) {
        complete_request(req, 0);
        return -EINVAL;
    }

    struct iov_iter it = {
        .iov = req->in.iov,
        .count = req->in.count,
    };
    /*
     * The guest may change the buffer content while the request is being
     * processed so make sure to only look into a copy.  For WRITEs only the
     * headers need to be copied, for the rest the whole request.
     */
    struct fuse_in_header in_hdr;
    size_t cplen = iov_iter_to_buf(&it, &in_hdr, sizeof(in_hdr));
    VHD_ASSERT(cplen == sizeof(in_hdr));

    // Don't process some requests with strange logic
    if (is_ignored_request(&in_hdr)) {
        complete_request(req, 0);
        return 0;
    }

    size_t buf0len = !is_write_request(&in_hdr) ? len :
        sizeof(struct fuse_in_header) + sizeof(struct fuse_write_in);

    req->buffer = vhd_alloc(buf0len);
    /* The guest may have changed the buffer, use the header copied earlier */
    memcpy(req->buffer, &in_hdr, sizeof(in_hdr));
    iov_iter_to_buf(&it, req->buffer + cplen, buf0len - cplen);

    struct fuse_bufvec bufv;
    size_t extra_bufs = it.count - it.idx;
    struct fuse_bufvec *pbufv = !extra_bufs ? &bufv :
        vhd_alloc(sizeof(*pbufv) + sizeof(struct fuse_buf) * extra_bufs);

    *pbufv = (struct fuse_bufvec) {
        .count = 1 + extra_bufs,
        .buf[0] = {
            .mem = req->buffer,
            .size = buf0len,
            .fd = -1,
        },
    };
    size_t idx;
    for (idx = 1; it.idx < it.count; idx++, it.idx++, it.off = 0) {
        pbufv->buf[idx] = (struct fuse_buf) {
            .mem = it.iov[it.idx].iov_base + it.off,
            .size = it.iov[it.idx].iov_len - it.off,
            .fd = -1,
        };
    }

    fuse_session_process_buf_int(se, pbufv, &req->ch);

    if (extra_bufs) {
        vhd_free(pbufv);
    }

    return 0;
}

static void unregister_complete(void* ctx)
{
    struct fuse_session* se = ctx;
    struct fuse_virtio_dev* dev = se->virtio_dev;
    int queue_index;

    VHD_LOG_INFO("stopping device %s", dev->fsdev.socket_path);
    for (queue_index = 0; queue_index < dev->rq_count; queue_index++) {
        vhd_stop_queue(dev->rqs[queue_index]);
    }
    VHD_LOG_INFO("finished stopping device %s", dev->fsdev.socket_path);
}

static void unregister_complete_and_notify(void* ctx)
{
    struct unregister_context *unreg_ctx = ctx;
    unregister_complete(unreg_ctx->se);
    atomic_store(&unreg_ctx->completed, true);
}

static void unregister_complete_and_free_dev(void* ctx)
{
    int queue_index;

    unregister_complete(ctx);

    struct fuse_session* se = ctx;
    struct fuse_virtio_dev* dev = se->virtio_dev;

    for (queue_index = 0; queue_index < dev->rq_count; queue_index++) {
        vhd_release_request_queue(dev->rqs[queue_index]);
    }
    vhd_free(dev->rqs);
    vhd_free(dev);
}

////////////////////////////////////////////////////////////////////////////////

uint64_t fuse_req_unique(fuse_req_t req)
{
    return req->unique;
}

void fuse_session_setparams(
    struct fuse_session* se,
    const struct fuse_session_params* params)
{
    se->conn.proto_major = params->proto_major;
    se->conn.proto_minor = params->proto_minor;
    se->conn.capable = params->capable;
    se->conn.want = params->want;
    se->bufsize = params->bufsize;

    se->got_init = 1;
    se->got_destroy = 0;
}

void fuse_session_getparams(
    struct fuse_session* se,
    struct fuse_session_params* params)
{
    params->proto_major = se->conn.proto_major;
    params->proto_minor = se->conn.proto_minor;
    params->capable = se->conn.capable;
    params->want = se->conn.want;
    params->bufsize = se->bufsize;
}

int fuse_cancel_request(
    fuse_req_t req,
    enum fuse_cancelation_code code)
{
    struct fuse_chan* ch = req->ch;
    struct fuse_virtio_request* vhd_req = VIRTIO_REQ_FROM_CHAN(ch);
    switch (code) {
        case FUSE_ERROR:
            fuse_reply_err(req, EINTR);
            break;
        case FUSE_SUSPEND:
            complete_request(vhd_req, VHD_BDEV_CANCELED);
            fuse_free_req(req);
            break;
    }
    return 0;
}

void fuse_reply_none(fuse_req_t req)
{
    struct fuse_chan* ch = req->ch;
    struct fuse_virtio_request* vhd_req = VIRTIO_REQ_FROM_CHAN(ch);
    complete_request(vhd_req, 0);

    fuse_free_req(req);
}

int virtio_session_mount(struct fuse_session* se)
{
    struct fuse_virtio_dev* dev = vhd_zalloc(sizeof(struct fuse_virtio_dev));
    int queue_index = 0;
    int ret = 0;

    // no need to supply tag here - it will be handled by the QEMU
    dev->fsdev.socket_path = se->vu_socket_path;
    dev->fsdev.num_queues = se->num_backend_queues;

    VHD_LOG_INFO(
        "starting device %s, num_backend_queues=%d, num_frontend_queues=%d",
        dev->fsdev.socket_path,
        se->num_backend_queues,
        se->num_frontend_queues);

    dev->rq_count = se->num_frontend_queues;
    dev->rqs = vhd_zalloc(sizeof(dev->rqs[0]) * dev->rq_count);

    for (queue_index = 0; queue_index < dev->rq_count; queue_index++) {
        dev->rqs[queue_index] = vhd_create_request_queue();
        if (!dev->rqs[queue_index]) {
            ret = -ENOMEM;
            goto clean;
        }
    }

    dev->vdev = vhd_register_fs_mq(&dev->fsdev, dev->rqs, dev->rq_count, NULL);
    if (!dev->vdev) {
        ret = -ENOMEM;
        goto clean;
    }

    int err = chmod(se->vu_socket_path, S_IRGRP | S_IWGRP | S_IRUSR | S_IWUSR);
    if (err < 0) {
        vhd_unregister_fs(dev->vdev, unregister_complete_and_free_dev, se);
        return err;
    }

    se->virtio_dev = dev;
    return 0;

clean:
    while (queue_index) {
        vhd_release_request_queue(dev->rqs[--queue_index]);
    }
    if (dev->rqs) {
        vhd_free(dev->rqs);
    }
    vhd_free(dev);

    return ret;
}

void virtio_session_close(struct fuse_session* se)
{
    struct fuse_virtio_dev* dev = se->virtio_dev;
    int queue_index;

    VHD_LOG_INFO("destroying device %s", dev->fsdev.socket_path);

    for (queue_index = 0; queue_index < dev->rq_count; queue_index++) {
        vhd_release_request_queue(dev->rqs[queue_index]);
    }
    vhd_free(dev->rqs);
    vhd_free(dev);

    VHD_LOG_INFO("finished destroying device");
}

void virtio_session_exit(struct fuse_session* se)
{
    struct unregister_context unreg_ctx = {
        .se = se,
        .completed = false
    };

    struct fuse_virtio_dev* dev = se->virtio_dev;

    VHD_LOG_INFO("unregister device %s", dev->fsdev.socket_path);
    vhd_unregister_fs(dev->vdev, unregister_complete_and_notify, &unreg_ctx);

    while (!atomic_load(&unreg_ctx.completed)) {
        sched_yield();
    }

    VHD_LOG_INFO("finished unregister device");
}

int virtio_session_loop(struct fuse_session* se, int queue_index)
{
    struct fuse_virtio_dev* dev = se->virtio_dev;

    int res;
    for (;;) {
        res = vhd_run_queue(dev->rqs[queue_index]);
        if (res != -EAGAIN) {
            if (res < 0) {
                VHD_LOG_WARN("request queue failure %d", -res);
            }
            break;
        }

        struct vhd_request req;
        while (vhd_dequeue_request(dev->rqs[queue_index], &req)) {
            res = process_request(se, req.io);
            if (res < 0) {
                VHD_LOG_WARN("request processing failure %d", -res);
            }
        }
    }

    se->exited = 1;
    return res;
}

int virtio_send_msg(
    struct fuse_session* se,
    struct fuse_chan* ch,
    struct iovec* iov,
    int count)
{
    Y_UNUSED(se);

    struct fuse_virtio_request* req = VIRTIO_REQ_FROM_CHAN(ch);

    VHD_ASSERT(count >= 1);
    VHD_ASSERT(iov[0].iov_len >= sizeof(struct fuse_out_header));

    size_t response_bytes = iov_size(iov, count);
    VHD_LOG_DEBUG("response with %d desc of length %zu\n",
        count, response_bytes);

    int error = 0;
    size_t out_bytes = iov_size(req->out.iov, req->out.count);
    if (response_bytes <= out_bytes) {
        iov_copy_to_iov(req->out.iov, req->out.count, iov, count, response_bytes);
    } else {
        VHD_LOG_ERROR("request buffers too small for response - "
                      "requested:%zu, available:%zu\n",
            response_bytes, out_bytes);
        error = -E2BIG;
    }

    complete_request(req, 0);
    return error;
}

int virtio_send_data_iov(
    struct fuse_session* se,
    struct fuse_chan* ch,
    struct iovec* iov,
    int count,
    struct fuse_bufvec* buf,
    size_t len)
{
    Y_UNUSED(se);
    Y_UNUSED(ch);
    Y_UNUSED(iov);
    Y_UNUSED(count);
    Y_UNUSED(buf);
    Y_UNUSED(len);

    // TODO
    return -1;
}

int virtio_out_buf(struct fuse_session *se, struct fuse_chan *ch,
                   struct iovec **iov, int *count)
{
    Y_UNUSED(se);

    struct fuse_virtio_request* req = VIRTIO_REQ_FROM_CHAN(ch);
    *iov = req->out.iov;
    *count = req->out.count;
    return 0;
}
