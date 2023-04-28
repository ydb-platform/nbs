#include "device_proxy.h"

#include "spdk.h"

namespace NCloud::NBlockStore::NSpdk {

namespace {

////////////////////////////////////////////////////////////////////////////////

int vbdev_proxy_module_init();
void vbdev_proxy_module_fini();
int vbdev_proxy_get_ctx_size();
int vbdev_proxy_config_json(spdk_json_write_ctx* w);
void vbdev_proxy_examine_config(spdk_bdev* bdev);

static spdk_bdev_module vbdev_proxy_module = {
    .module_init        = vbdev_proxy_module_init,
    .module_fini        = vbdev_proxy_module_fini,
    .config_json        = vbdev_proxy_config_json,
    .name               = "proxy",
    .get_ctx_size       = vbdev_proxy_get_ctx_size,
    .examine_config     = vbdev_proxy_examine_config,
};

SPDK_BDEV_MODULE_REGISTER(proxy, &vbdev_proxy_module)

////////////////////////////////////////////////////////////////////////////////

int vbdev_proxy_destruct(void* ctx);
bool vbdev_proxy_io_type_supported(void* ctx, spdk_bdev_io_type io_type);
spdk_io_channel* vbdev_proxy_get_io_channel(void* ctx);
void vbdev_proxy_submit_request(spdk_io_channel* ch, spdk_bdev_io* bdev_io);
int vbdev_proxy_dump_info_json(void* ctx, spdk_json_write_ctx* w);
void vbdev_proxy_write_config_json(spdk_bdev* bdev, spdk_json_write_ctx* w);

static spdk_bdev_fn_table vbdev_proxy_fn_table = {
    .destruct           = vbdev_proxy_destruct,
    .submit_request     = vbdev_proxy_submit_request,
    .io_type_supported  = vbdev_proxy_io_type_supported,
    .get_io_channel     = vbdev_proxy_get_io_channel,
    .dump_info_json     = vbdev_proxy_dump_info_json,
    .write_config_json  = vbdev_proxy_write_config_json,
};

////////////////////////////////////////////////////////////////////////////////

struct vbdev_proxy_node
{
    spdk_bdev*      base_bdev;
    spdk_bdev_desc* base_desc;
    spdk_bdev       bdev;
    TAILQ_ENTRY(vbdev_proxy_node) link;
};

TAILQ_HEAD(, vbdev_proxy_node) g_nodes = TAILQ_HEAD_INITIALIZER(g_nodes);

vbdev_proxy_node* vbdev_proxy_node_alloc()
{
    return static_cast<vbdev_proxy_node*>(calloc(1, sizeof(vbdev_proxy_node)));
}

void vbdev_proxy_node_free(vbdev_proxy_node* node)
{
    free(node->bdev.name);
    free(node);
}

vbdev_proxy_node* vbdev_proxy_node_from_ctx(void* ctx)
{
    return static_cast<vbdev_proxy_node*>(ctx);
}

vbdev_proxy_node* vbdev_proxy_node_from_bdev(spdk_bdev* bdev)
{
    return SPDK_CONTAINEROF(bdev, vbdev_proxy_node, bdev);
}

////////////////////////////////////////////////////////////////////////////////

struct vbdev_proxy_io_channel
{
    spdk_io_channel* base_ch;
};

vbdev_proxy_io_channel* vbdev_proxy_io_channel_from_ctx(void* ctx)
{
    return static_cast<vbdev_proxy_io_channel*>(ctx);
}

////////////////////////////////////////////////////////////////////////////////

struct vbdev_proxy_io
{
    spdk_io_channel* ch;
    spdk_bdev_io_wait_entry bdev_io_wait;
};

vbdev_proxy_io* vbdev_proxy_io_from_ctx(void* ctx)
{
    return static_cast<vbdev_proxy_io*>(ctx);
}

////////////////////////////////////////////////////////////////////////////////

int vbdev_proxy_module_init()
{
    return 0;
}

void vbdev_proxy_module_fini()
{
}

int vbdev_proxy_get_ctx_size()
{
    return sizeof(vbdev_proxy_io);
}

int vbdev_proxy_config_json(spdk_json_write_ctx* w)
{
    Y_UNUSED(w);
    return 0;
}

void vbdev_proxy_examine_config(spdk_bdev* bdev)
{
    Y_UNUSED(bdev);

    spdk_bdev_module_examine_done(&vbdev_proxy_module);
}

////////////////////////////////////////////////////////////////////////////////

int vbdev_proxy_io_channel_create_cb(void* io_device, void* ctx_buf)
{
    auto* node = vbdev_proxy_node_from_ctx(io_device);

    auto* channel = vbdev_proxy_io_channel_from_ctx(ctx_buf);
    channel->base_ch = spdk_bdev_get_io_channel(node->base_desc);

    return 0;
}

void vbdev_proxy_io_channel_destroy_cb(void* io_device, void* ctx_buf)
{
    Y_UNUSED(io_device);

    auto* channel = vbdev_proxy_io_channel_from_ctx(ctx_buf);
    spdk_put_io_channel(channel->base_ch);
}

int vbdev_proxy_construct(
    spdk_bdev* bdev,
    spdk_bdev_desc* bdev_desc,
    const char* name)
{
    int rc = 0;

    auto* node = vbdev_proxy_node_alloc();
    if (!node) {
        SPDK_ERRLOG("could not allocate proxy bdev");
        rc = -ENOMEM;
        goto error;
    }

    node->base_bdev = bdev;
    node->base_desc = bdev_desc;

    node->bdev.name = strdup(name);
    if (!node->bdev.name) {
        SPDK_ERRLOG("could not allocate proxy bdev");
        rc = -ENOMEM;
        goto error_free;
    }

    node->bdev.product_name = const_cast<char*>(vbdev_proxy_module.name);
    node->bdev.module = &vbdev_proxy_module;
    node->bdev.fn_table = &vbdev_proxy_fn_table;
    node->bdev.ctxt = node;

    // just copy device settings
    node->bdev.write_cache = bdev->write_cache;
    node->bdev.required_alignment = bdev->required_alignment;
    node->bdev.optimal_io_boundary = bdev->optimal_io_boundary;
    node->bdev.blocklen = bdev->blocklen;
    node->bdev.blockcnt = bdev->blockcnt;

    node->bdev.md_interleave = bdev->md_interleave;
    node->bdev.md_len = bdev->md_len;
    node->bdev.dif_type = bdev->dif_type;
    node->bdev.dif_is_head_of_md = bdev->dif_is_head_of_md;
    node->bdev.dif_check_flags = bdev->dif_check_flags;

    TAILQ_INSERT_TAIL(&g_nodes, node, link);
    spdk_io_device_register(
        node,
        vbdev_proxy_io_channel_create_cb,
        vbdev_proxy_io_channel_destroy_cb,
        sizeof(vbdev_proxy_io_channel),
        node->bdev.name);

    rc = spdk_bdev_register(&node->bdev);

    if (rc) {
        SPDK_ERRLOG("could not register proxy bdev");
        goto error_unregister;
    }

    return 0;

error_unregister:
    TAILQ_REMOVE(&g_nodes, node, link);
    spdk_io_device_unregister(node, nullptr);

error_free:
    vbdev_proxy_node_free(node);

error:
    return rc;
}

void vbdev_proxy_io_device_unregister_cb(void* arg)
{
    auto* node = vbdev_proxy_node_from_ctx(arg);
    vbdev_proxy_node_free(node);
}

int vbdev_proxy_destruct(void* ctx)
{
    auto* node = vbdev_proxy_node_from_ctx(ctx);
    TAILQ_REMOVE(&g_nodes, node, link);

    spdk_bdev_module_release_bdev(node->base_bdev);
    spdk_bdev_close(node->base_desc);

    spdk_io_device_unregister(node, vbdev_proxy_io_device_unregister_cb);

    return 0;
}

void vbdev_proxy_base_bdev_event_cb(
    enum spdk_bdev_event_type type,
    spdk_bdev *bdev,
    void* ctx)
{
    Y_UNUSED(ctx);

    switch (type) {
    case SPDK_BDEV_EVENT_REMOVE:
        vbdev_proxy_node* node;
        vbdev_proxy_node* tmp;

        TAILQ_FOREACH_SAFE(node, &g_nodes, link, tmp) {
            if (node->base_bdev == bdev) {
                spdk_bdev_unregister(&node->bdev, nullptr, nullptr);
            }
        }
        break;

    case SPDK_BDEV_EVENT_RESIZE:
    case SPDK_BDEV_EVENT_MEDIA_MANAGEMENT:
        break;
    }
}

bool vbdev_proxy_io_type_supported(void* ctx, spdk_bdev_io_type io_type)
{
    auto* node = vbdev_proxy_node_from_ctx(ctx);
    return spdk_bdev_io_type_supported(node->base_bdev, io_type);
}

spdk_io_channel* vbdev_proxy_get_io_channel(void* ctx)
{
    auto* node = vbdev_proxy_node_from_ctx(ctx);
    return spdk_get_io_channel(node);
}

void vbdev_proxy_queue_io_wait_cb(void* arg)
{
    auto* bdev_io = static_cast<spdk_bdev_io*>(arg);
    auto* io_ctx = vbdev_proxy_io_from_ctx(bdev_io->driver_ctx);

    vbdev_proxy_submit_request(io_ctx->ch, bdev_io);
}

void vbdev_proxy_queue_io(spdk_bdev_io* bdev_io)
{
    auto* io_ctx = vbdev_proxy_io_from_ctx(bdev_io->driver_ctx);

    io_ctx->bdev_io_wait.bdev = bdev_io->bdev;
    io_ctx->bdev_io_wait.cb_fn = vbdev_proxy_queue_io_wait_cb;
    io_ctx->bdev_io_wait.cb_arg = bdev_io;

    int rc = spdk_bdev_queue_io_wait(
        bdev_io->bdev,
        io_ctx->ch,
        &io_ctx->bdev_io_wait);

    if (rc) {
        spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
    }
}

void vbdev_proxy_complete_io(spdk_bdev_io* bdev_io, bool success, void* arg)
{
    auto status = success ? SPDK_BDEV_IO_STATUS_SUCCESS : SPDK_BDEV_IO_STATUS_FAILED;
    auto* orig_io = static_cast<spdk_bdev_io*>(arg);

    spdk_bdev_io_complete(orig_io, status);
    spdk_bdev_free_io(bdev_io);
}

void vbdev_proxy_complete_zcopy_io(spdk_bdev_io* bdev_io, bool success, void* arg)
{
    auto status = success ? SPDK_BDEV_IO_STATUS_SUCCESS : SPDK_BDEV_IO_STATUS_FAILED;
    auto* orig_io = static_cast<spdk_bdev_io*>(arg);

    spdk_bdev_io_set_buf(
        orig_io,
        bdev_io->u.bdev.iovs[0].iov_base,
        bdev_io->u.bdev.iovs[0].iov_len);

    spdk_bdev_io_complete(orig_io, status);
    spdk_bdev_free_io(bdev_io);
}

void vbdev_proxy_submit_request(spdk_io_channel* ch, spdk_bdev_io* bdev_io)
{
    auto* node = vbdev_proxy_node_from_bdev(bdev_io->bdev);
    auto* io_ctx = vbdev_proxy_io_from_ctx(bdev_io->driver_ctx);

    void* channel_ctx = spdk_io_channel_get_ctx(ch);
    auto* channel = vbdev_proxy_io_channel_from_ctx(channel_ctx);

    int rc;

    switch (bdev_io->type) {
    case SPDK_BDEV_IO_TYPE_READ:
        if (!bdev_io->u.bdev.md_buf) {
            rc = spdk_bdev_readv_blocks(
                node->base_desc,
                channel->base_ch,
                bdev_io->u.bdev.iovs,
                bdev_io->u.bdev.iovcnt,
                bdev_io->u.bdev.offset_blocks,
                bdev_io->u.bdev.num_blocks,
                vbdev_proxy_complete_io,
                bdev_io);
        } else {
            rc = spdk_bdev_readv_blocks_with_md(
                node->base_desc,
                channel->base_ch,
                bdev_io->u.bdev.iovs,
                bdev_io->u.bdev.iovcnt,
                bdev_io->u.bdev.md_buf,
                bdev_io->u.bdev.offset_blocks,
                bdev_io->u.bdev.num_blocks,
                vbdev_proxy_complete_io,
                bdev_io);
        }
        break;

    case SPDK_BDEV_IO_TYPE_WRITE:
        if (!bdev_io->u.bdev.md_buf) {
            rc = spdk_bdev_writev_blocks(
                node->base_desc,
                channel->base_ch,
                bdev_io->u.bdev.iovs,
                bdev_io->u.bdev.iovcnt,
                bdev_io->u.bdev.offset_blocks,
                bdev_io->u.bdev.num_blocks,
                vbdev_proxy_complete_io,
                bdev_io);
        } else {
            rc = spdk_bdev_writev_blocks_with_md(
                node->base_desc,
                channel->base_ch,
                bdev_io->u.bdev.iovs,
                bdev_io->u.bdev.iovcnt,
                bdev_io->u.bdev.md_buf,
                bdev_io->u.bdev.offset_blocks,
                bdev_io->u.bdev.num_blocks,
                vbdev_proxy_complete_io,
                bdev_io);
        }
        break;

    case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
        rc = spdk_bdev_write_zeroes_blocks(
            node->base_desc,
            channel->base_ch,
            bdev_io->u.bdev.offset_blocks,
            bdev_io->u.bdev.num_blocks,
            vbdev_proxy_complete_io,
            bdev_io);
        break;

    case SPDK_BDEV_IO_TYPE_UNMAP:
        rc = spdk_bdev_unmap_blocks(
            node->base_desc,
            channel->base_ch,
            bdev_io->u.bdev.offset_blocks,
            bdev_io->u.bdev.num_blocks,
            vbdev_proxy_complete_io,
            bdev_io);
        break;

    case SPDK_BDEV_IO_TYPE_FLUSH:
        rc = spdk_bdev_flush_blocks(
            node->base_desc,
            channel->base_ch,
            bdev_io->u.bdev.offset_blocks,
            bdev_io->u.bdev.num_blocks,
            vbdev_proxy_complete_io,
            bdev_io);
        break;

    case SPDK_BDEV_IO_TYPE_RESET:
        rc = spdk_bdev_reset(
            node->base_desc,
            channel->base_ch,
            vbdev_proxy_complete_io,
            bdev_io);
        break;

    case SPDK_BDEV_IO_TYPE_ZCOPY:
        rc = spdk_bdev_zcopy_start(
            node->base_desc,
            channel->base_ch,
            bdev_io->u.bdev.offset_blocks,
            bdev_io->u.bdev.num_blocks,
            bdev_io->u.bdev.zcopy.populate,
            vbdev_proxy_complete_zcopy_io,
            bdev_io);
        break;

    default:
        rc = -ENOTSUP; // unsupported request
        break;
    }

    if (rc) {
        if (rc == -ENOMEM) {
            io_ctx->ch = ch;
            vbdev_proxy_queue_io(bdev_io);
            return;
        }

        spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
    }
}

int vbdev_proxy_dump_info_json(void* ctx, spdk_json_write_ctx* w)
{
    Y_UNUSED(ctx);
    Y_UNUSED(w);
    return 0;
}

void vbdev_proxy_write_config_json(spdk_bdev* bdev, spdk_json_write_ctx* w)
{
    Y_UNUSED(bdev);
    Y_UNUSED(w);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int RegisterDeviceProxy(spdk_bdev* bdev, const TString& name)
{
    spdk_bdev_desc* bdev_desc;
    const char* bdev_name = spdk_bdev_get_name(bdev);

    int rc = spdk_bdev_open_ext(
        bdev_name,
        true,
        vbdev_proxy_base_bdev_event_cb,
        nullptr,
        &bdev_desc);

    if (rc) {
        SPDK_ERRLOG("could not open bdev %s\n", bdev_name);
        goto error;
    }

    rc = spdk_bdev_module_claim_bdev(
        bdev,
        bdev_desc,
        &vbdev_proxy_module);

    if (rc) {
        SPDK_ERRLOG("could not claim bdev %s\n", bdev_name);
        goto error_close;
    }

    rc = vbdev_proxy_construct(bdev, bdev_desc, name.c_str());

    if (rc) {
        SPDK_ERRLOG("could not construct proxy bdev");
        goto error_release;
    }

    return 0;

error_release:
    spdk_bdev_module_release_bdev(bdev);

error_close:
    spdk_bdev_close(bdev_desc);

error:
    return rc;
}

}   // namespace NCloud::NBlockStore::NSpdk
