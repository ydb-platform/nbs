#include "device_wrapper.h"

#include "device.h"
#include "spdk.h"

namespace NCloud::NBlockStore::NSpdk {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

TSgList CreateSglist(iovec* iov, int iovcnt)
{
    auto* begin = reinterpret_cast<TBlockDataRef*>(iov);
    return { begin, begin + iovcnt };
}

////////////////////////////////////////////////////////////////////////////////

int vbdev_wrapper_module_init();
void vbdev_wrapper_module_fini();
int vbdev_wrapper_get_ctx_size();

static spdk_bdev_module vbdev_wrapper_module = {
    .module_init        = vbdev_wrapper_module_init,
    .module_fini        = vbdev_wrapper_module_fini,
    .name               = "wrapper",
    .get_ctx_size       = vbdev_wrapper_get_ctx_size,
};

SPDK_BDEV_MODULE_REGISTER(wrapper, &vbdev_wrapper_module)

////////////////////////////////////////////////////////////////////////////////

int vbdev_wrapper_destruct(void* ctx);
bool vbdev_wrapper_io_type_supported(void* ctx, spdk_bdev_io_type io_type);
spdk_io_channel* vbdev_wrapper_get_io_channel(void* ctx);
void vbdev_wrapper_submit_request(spdk_io_channel* ch, spdk_bdev_io* bdev_io);

static spdk_bdev_fn_table vbdev_wrapper_fn_table = {
    .destruct           = vbdev_wrapper_destruct,
    .submit_request     = vbdev_wrapper_submit_request,
    .io_type_supported  = vbdev_wrapper_io_type_supported,
    .get_io_channel     = vbdev_wrapper_get_io_channel,
};

////////////////////////////////////////////////////////////////////////////////

struct vbdev_wrapper_node
{
    ISpdkDevicePtr device;
    TString name;

    spdk_bdev bdev;
    TAILQ_ENTRY(vbdev_wrapper_node) link;
};

TAILQ_HEAD(, vbdev_wrapper_node) g_nodes = TAILQ_HEAD_INITIALIZER(g_nodes);

vbdev_wrapper_node* vbdev_wrapper_node_alloc()
{
    return new vbdev_wrapper_node();
}

void vbdev_wrapper_node_free(vbdev_wrapper_node* node)
{
    if (node->device) {
        node->device->StopAsync();
    }
    delete node;
}

vbdev_wrapper_node* vbdev_wrapper_node_from_ctx(void* ctx)
{
    return static_cast<vbdev_wrapper_node*>(ctx);
}

vbdev_wrapper_node* vbdev_wrapper_node_from_bdev(spdk_bdev* bdev)
{
    return SPDK_CONTAINEROF(bdev, vbdev_wrapper_node, bdev);
}

////////////////////////////////////////////////////////////////////////////////

struct vbdev_wrapper_io_request
{
    TFuture<NProto::TError> Response;

    TAILQ_ENTRY(vbdev_wrapper_io_request) link;
};

vbdev_wrapper_io_request* vbdev_wrapper_io_request_init(void* ctx)
{
    return new (ctx) vbdev_wrapper_io_request();
}

void vbdev_wrapper_io_request_destroy(vbdev_wrapper_io_request* request)
{
    request->~vbdev_wrapper_io_request();
}

spdk_bdev_io_status vbdev_wrapper_io_request_status(
    vbdev_wrapper_io_request* request)
{
    auto error = ExtractResponse(request->Response);
    if (SUCCEEDED(error.GetCode())) {
        return SPDK_BDEV_IO_STATUS_SUCCESS;
    } else {
        return SPDK_BDEV_IO_STATUS_FAILED;
    }
}

void vbdev_wrapper_io_request_complete(
    vbdev_wrapper_io_request* request,
    spdk_bdev_io_status status)
{
    auto* bdev_io = spdk_bdev_io_from_ctx(request);

    vbdev_wrapper_io_request_destroy(request);
    spdk_bdev_io_complete(bdev_io, status);
}

////////////////////////////////////////////////////////////////////////////////

struct vbdev_wrapper_io_channel
{
    struct vbdev_wrapper_group_channel* group_ch;

    TAILQ_HEAD(, vbdev_wrapper_io_request) requests;
    pthread_mutex_t requests_mutex;

    TAILQ_ENTRY(vbdev_wrapper_io_channel) link;
};

vbdev_wrapper_io_channel* vbdev_wrapper_io_channel_from_ctx(void* ctx)
{
    return static_cast<vbdev_wrapper_io_channel*>(ctx);
}

////////////////////////////////////////////////////////////////////////////////

struct vbdev_wrapper_group_channel
{
    struct spdk_poller* poller;
    TAILQ_HEAD(, vbdev_wrapper_io_channel) channels;
};

vbdev_wrapper_group_channel* vbdev_wrapper_group_channel_from_ctx(void* ctx)
{
    return static_cast<vbdev_wrapper_group_channel*>(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void vbdev_wrapper_io_channel_post(
    vbdev_wrapper_io_channel* ch,
    vbdev_wrapper_io_request* request)
{
    // TODO: lock-free single-consumer queue
    pthread_mutex_lock(&ch->requests_mutex);
    TAILQ_INSERT_TAIL(&ch->requests, request, link);
    pthread_mutex_unlock(&ch->requests_mutex);
}

int vbdev_wrapper_io_channel_poll(vbdev_wrapper_io_channel* ch)
{
    TAILQ_HEAD(, vbdev_wrapper_io_request) requests = TAILQ_HEAD_INITIALIZER(requests);

    if (pthread_mutex_trylock(&ch->requests_mutex) == 0) {
        // grab all pending requests
        TAILQ_CONCAT(&requests, &ch->requests, link);
        pthread_mutex_unlock(&ch->requests_mutex);
    } else {
        // should return SPDK_POLLER_BUSY if we miss channel on this round
        return 1;
    }

    // complete requests
    int count = 0;

    vbdev_wrapper_io_request *request, *tmp;
    TAILQ_FOREACH_SAFE(request, &requests, link, tmp) {
        auto status = vbdev_wrapper_io_request_status(request);
        vbdev_wrapper_io_request_complete(request, status);

        ++count;
    }

    return count;
}

int vbdev_wrapper_io_channel_create(void* io_device, void* ctx_buf)
{
    Y_UNUSED(io_device);

    auto* ch = vbdev_wrapper_io_channel_from_ctx(ctx_buf);

    TAILQ_INIT(&ch->requests);
    pthread_mutex_init(&ch->requests_mutex, NULL);

    ch->group_ch = vbdev_wrapper_group_channel_from_ctx(
        spdk_io_channel_get_ctx(spdk_get_io_channel(&vbdev_wrapper_module)));

    TAILQ_INSERT_TAIL(&ch->group_ch->channels, ch, link);
    return 0;
}

void vbdev_wrapper_io_channel_destroy(void* io_device, void* ctx_buf)
{
    Y_UNUSED(io_device);

    auto* ch = vbdev_wrapper_io_channel_from_ctx(ctx_buf);

    TAILQ_REMOVE(&ch->group_ch->channels, ch, link);
    spdk_put_io_channel(spdk_io_channel_from_ctx(ch->group_ch));

    if (!TAILQ_EMPTY(&ch->requests)) {
        SPDK_ERRLOG("IO channel of bdev wrapper has uncleared IO request");
    }
    pthread_mutex_destroy(&ch->requests_mutex);
}

////////////////////////////////////////////////////////////////////////////////

int vbdev_wrapper_group_channel_poll(void* ctx)
{
    auto* group_ch = vbdev_wrapper_group_channel_from_ctx(ctx);
    int count = 0;

    vbdev_wrapper_io_channel* ch;
    TAILQ_FOREACH(ch, &group_ch->channels, link) {
        count += vbdev_wrapper_io_channel_poll(ch);
    }

    return count > 0 ? SPDK_POLLER_BUSY : SPDK_POLLER_IDLE;
}

int vbdev_wrapper_group_channel_create(void* io_device, void* ctx_buf)
{
    Y_UNUSED(io_device);

    auto* group_ch = vbdev_wrapper_group_channel_from_ctx(ctx_buf);
    TAILQ_INIT(&group_ch->channels);

    group_ch->poller = SPDK_POLLER_REGISTER(vbdev_wrapper_group_channel_poll, group_ch, 0);
    return 0;
}

void vbdev_wrapper_group_channel_destroy(void* io_device, void* ctx_buf)
{
    Y_UNUSED(io_device);

    auto* group_ch = vbdev_wrapper_group_channel_from_ctx(ctx_buf);
    if (!TAILQ_EMPTY(&group_ch->channels)) {
        SPDK_ERRLOG("Group channel of bdev wrapper has uncleared IO channel");
    }

    spdk_poller_unregister(&group_ch->poller);
}

////////////////////////////////////////////////////////////////////////////////

int vbdev_wrapper_module_init()
{
    spdk_io_device_register(
        &vbdev_wrapper_module,
        vbdev_wrapper_group_channel_create,
        vbdev_wrapper_group_channel_destroy,
        sizeof(vbdev_wrapper_group_channel),
        vbdev_wrapper_module.name);

    return 0;
}

void vbdev_wrapper_module_fini()
{
    spdk_io_device_unregister(&vbdev_wrapper_module, NULL);
}

int vbdev_wrapper_get_ctx_size()
{
    return sizeof(vbdev_wrapper_io_request);
}

bool vbdev_wrapper_io_type_supported(void* ctx, spdk_bdev_io_type io_type)
{
    Y_UNUSED(ctx);

    switch (io_type) {
        case SPDK_BDEV_IO_TYPE_READ:
        case SPDK_BDEV_IO_TYPE_WRITE:
        case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
            return true;

        default:
            return false;
    }
}

spdk_io_channel* vbdev_wrapper_get_io_channel(void* ctx)
{
    auto* node = vbdev_wrapper_node_from_ctx(ctx);
    return spdk_get_io_channel(node);
}

void vbdev_wrapper_execute_io_request(spdk_io_channel* io_ch, spdk_bdev_io* bdev_io)
{
    auto* ch = vbdev_wrapper_io_channel_from_ctx(
        spdk_io_channel_get_ctx(io_ch));

    auto* node = vbdev_wrapper_node_from_bdev(bdev_io->bdev);
    auto* request = vbdev_wrapper_io_request_init(bdev_io->driver_ctx);

    switch (bdev_io->type) {
    case SPDK_BDEV_IO_TYPE_READ:
        request->Response = node->device->Read(
            CreateSglist(bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt),
            bdev_io->u.bdev.offset_blocks * node->bdev.blocklen,
            bdev_io->u.bdev.num_blocks * node->bdev.blocklen);
        break;

    case SPDK_BDEV_IO_TYPE_WRITE:
        request->Response = node->device->Write(
            CreateSglist(bdev_io->u.bdev.iovs, bdev_io->u.bdev.iovcnt),
            bdev_io->u.bdev.offset_blocks * node->bdev.blocklen,
            bdev_io->u.bdev.num_blocks * node->bdev.blocklen);
        break;

    case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
        request->Response = node->device->WriteZeroes(
            bdev_io->u.bdev.offset_blocks * node->bdev.blocklen,
            bdev_io->u.bdev.num_blocks * node->bdev.blocklen);
        break;

    default:
        Y_FAIL("unexpected io-request type");
        return;
    }

    if (request->Response.HasValue()) {
        auto status = vbdev_wrapper_io_request_status(request);
        vbdev_wrapper_io_request_complete(request, status);
    } else {
        request->Response.Subscribe([=] (const auto&) {
            vbdev_wrapper_io_channel_post(ch, request);
        });
    }
}

void vbdev_wrapper_get_buf_cb(
    struct spdk_io_channel *io_ch,
    struct spdk_bdev_io *bdev_io,
    bool success)
{
    if (!success) {
        spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
        return;
    }

    vbdev_wrapper_execute_io_request(io_ch, bdev_io);
}

void vbdev_wrapper_submit_request(spdk_io_channel* io_ch, spdk_bdev_io* bdev_io)
{
    Y_VERIFY(bdev_io->num_retries == 0);

    switch (bdev_io->type) {
    case SPDK_BDEV_IO_TYPE_READ:
        if (bdev_io->u.bdev.iovs && bdev_io->u.bdev.iovs[0].iov_base) {
            vbdev_wrapper_execute_io_request(io_ch, bdev_io);
        } else {
            auto* node = vbdev_wrapper_node_from_bdev(bdev_io->bdev);
            spdk_bdev_io_get_buf(bdev_io, vbdev_wrapper_get_buf_cb,
                bdev_io->u.bdev.num_blocks * node->bdev.blocklen);
        }
        break;

    case SPDK_BDEV_IO_TYPE_WRITE:
    case SPDK_BDEV_IO_TYPE_WRITE_ZEROES:
        vbdev_wrapper_execute_io_request(io_ch, bdev_io);
        break;

    default:
        // unsupported request
        spdk_bdev_io_complete(bdev_io, SPDK_BDEV_IO_STATUS_FAILED);
        return;
    }
}

void vbdev_wrapper_io_device_unregister(void* ctx)
{
    auto* node = vbdev_wrapper_node_from_ctx(ctx);
    vbdev_wrapper_node_free(node);
}

int vbdev_wrapper_destruct(void* ctx)
{
    auto* node = vbdev_wrapper_node_from_ctx(ctx);

    TAILQ_REMOVE(&g_nodes, node, link);
    spdk_io_device_unregister(node, vbdev_wrapper_io_device_unregister);

    return 0;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

int RegisterDeviceWrapper(
    ISpdkDevicePtr device,
    TString name,
    ui64 blocksCount,
    ui32 blockSize)
{
    auto* node = vbdev_wrapper_node_alloc();
    if (!node) {
        SPDK_ERRLOG("could not allocate wrapper bdev");
        return -ENOMEM;
    }

    node->device = std::move(device);
    node->name = std::move(name);

    node->bdev.name = const_cast<char*>(node->name.c_str());
    node->bdev.product_name = const_cast<char*>(vbdev_wrapper_module.name);
    node->bdev.module = &vbdev_wrapper_module;
    node->bdev.fn_table = &vbdev_wrapper_fn_table;
    node->bdev.ctxt = node;

    node->bdev.blocklen = blockSize;
    node->bdev.blockcnt = blocksCount;

    // TODO
    // node->bdev.write_cache = bdev->write_cache;
    // node->bdev.required_alignment = bdev->required_alignment;
    // node->bdev.optimal_io_boundary = bdev->optimal_io_boundary;

    // node->bdev.md_interleave = bdev->md_interleave;
    // node->bdev.md_len = bdev->md_len;

    // node->bdev.dif_type = bdev->dif_type;
    // node->bdev.dif_is_head_of_md = bdev->dif_is_head_of_md;
    // node->bdev.dif_check_flags = bdev->dif_check_flags;

    TAILQ_INSERT_TAIL(&g_nodes, node, link);
    spdk_io_device_register(
        node,
        vbdev_wrapper_io_channel_create,
        vbdev_wrapper_io_channel_destroy,
        sizeof(vbdev_wrapper_io_channel),
        node->bdev.name);

    int rc = spdk_bdev_register(&node->bdev);
    if (rc) {
        SPDK_ERRLOG("could not register wrapper bdev");

        TAILQ_REMOVE(&g_nodes, node, link);
        spdk_io_device_unregister(node, nullptr);

        vbdev_wrapper_node_free(node);
    }

    return rc;
}

}   // namespace NCloud::NBlockStore::NSpdk
