#include "spdk/fsdev.h"
#include "spdk/fsdev_module.h"
#include "spdk/json.h"
#include "spdk/jsonrpc.h"
#include "spdk/rpc.h"
#include "spdk/thread.h"
#include "spdk/util.h"

#include <sys/stat.h>

#define FILESTORE_ROOT_INO 1
#define FILESTORE_HELLO_INO 2

#define FILESTORE_ROOT_FH 1
#define FILESTORE_HELLO_FH 2

#define FILESTORE_DEFAULT_MAX_XFER_SIZE (128 * 1024)
#define FILESTORE_DEFAULT_MAX_READAHEAD (128 * 1024)

static const char g_hello_name[] = "hello.txt";
static const char g_hello_data[] = "hello world\n";

struct filestore_fsdev {
    struct spdk_fsdev fsdev;
};

static struct spdk_fsdev_module g_filestore_module;
static char g_filestore_io_device;

static int
filestore_channel_create_cb(void *io_device, void *ctx_buf)
{
    (void)io_device;
    (void)ctx_buf;
    return 0;
}

static void
filestore_channel_destroy_cb(void *io_device, void *ctx_buf)
{
    (void)io_device;
    (void)ctx_buf;
}

static int
filestore_module_init(void)
{
    spdk_io_device_register(
        &g_filestore_io_device,
        filestore_channel_create_cb,
        filestore_channel_destroy_cb,
        0,
        "filestore_fsdev");

    return 0;
}

static void
filestore_module_fini(void)
{
    spdk_io_device_unregister(&g_filestore_io_device, NULL);
}

static int
filestore_get_ctx_size(void)
{
    return 0;
}

static void
filestore_fill_attr(uint64_t ino, struct fuse_attr *attr)
{
    memset(attr, 0, sizeof(*attr));

    attr->ino = ino;
    attr->nlink = 1;
    attr->uid = geteuid();
    attr->gid = getegid();
    attr->blksize = 4096;

    if (ino == FILESTORE_ROOT_INO) {
        attr->mode = S_IFDIR | 0555;
        attr->nlink = 2;
        attr->size = 0;
        attr->blocks = 0;
    } else {
        attr->mode = S_IFREG | 0444;
        attr->size = sizeof(g_hello_data) - 1;
        attr->blocks = (attr->size + 511) / 512;
    }
}

static int
filestore_node_exists(uint64_t ino)
{
    return ino == FILESTORE_ROOT_INO || ino == FILESTORE_HELLO_INO;
}

static void
filestore_fill_attr_out(uint64_t ino, struct fuse_attr_out *out)
{
    memset(out, 0, sizeof(*out));
    out->attr_valid = 1;
    filestore_fill_attr(ino, &out->attr);
}

static void
filestore_fill_entry_out(uint64_t ino, struct fuse_entry_out *out)
{
    memset(out, 0, sizeof(*out));
    out->nodeid = ino;
    out->generation = 1;
    out->entry_valid = 1;
    out->attr_valid = 1;
    filestore_fill_attr(ino, &out->attr);
}

static void
filestore_io_complete(struct spdk_fsdev_io *fsdev_io, int status)
{
    struct fuse_out_header *out_hdr = fsdev_io->u_out.fuse.hdr;

    if (out_hdr != NULL) {
        out_hdr->error = status;
    }

    spdk_fsdev_io_complete(fsdev_io, status);
}

static void
filestore_init_out_header(struct spdk_fsdev_io *fsdev_io)
{
    struct fuse_out_header *out_hdr = fsdev_io->u_out.fuse.hdr;

    if (out_hdr == NULL) {
        return;
    }

    memset(out_hdr, 0, sizeof(*out_hdr));
    out_hdr->unique = fsdev_io->u_in.fuse.hdr->unique;
    out_hdr->len = sizeof(*out_hdr);
}

static uint32_t
filestore_min_u32(uint32_t lhs, uint32_t rhs)
{
    return lhs < rhs ? lhs : rhs;
}

static int
filestore_copy_name(struct spdk_fsdev_io *fsdev_io, char *buf, size_t buf_size)
{
    size_t len;

    if (buf_size == 0) {
        return -EINVAL;
    }

    len = spdk_iov_length(fsdev_io->u_in.fuse.iov, fsdev_io->u_in.fuse.iovcnt);
    if (len == 0 || len >= buf_size) {
        return -EINVAL;
    }

    spdk_copy_iovs_to_buf(buf, len, fsdev_io->u_in.fuse.iov, fsdev_io->u_in.fuse.iovcnt);
    buf[len] = '\0';

    return 0;
}

static int
filestore_op_init(struct spdk_fsdev_io *fsdev_io)
{
    const struct fuse_init_in *init_in = fsdev_io->u_in.fuse.op.init;
    struct fuse_init_out *init_out = fsdev_io->u_out.fuse.op.init;
    struct fuse_out_header *out_hdr = fsdev_io->u_out.fuse.hdr;
    uint32_t max_xfer_size;
    uint32_t max_segments;
    uint64_t supported_flags;
    uint64_t requested_flags;
    uint64_t accepted_flags;

    if (init_in == NULL || init_out == NULL || out_hdr == NULL) {
        return -EINVAL;
    }

    memset(init_out, 0, sizeof(*init_out));

    init_out->major = FUSE_KERNEL_VERSION;
    init_out->minor = filestore_min_u32(init_in->minor, FUSE_KERNEL_MINOR_VERSION);
    init_out->max_readahead = filestore_min_u32(
        init_in->max_readahead,
        FILESTORE_DEFAULT_MAX_READAHEAD);
    init_out->max_background = 64;
    init_out->congestion_threshold = 48;

    max_xfer_size = spdk_fsdev_io_get_max_xfer_size(fsdev_io);
    if (max_xfer_size == 0 || max_xfer_size > FILESTORE_DEFAULT_MAX_XFER_SIZE) {
        max_xfer_size = FILESTORE_DEFAULT_MAX_XFER_SIZE;
    }
    init_out->max_write = max_xfer_size;

    max_segments = spdk_fsdev_io_get_max_segments(fsdev_io);
    if (max_segments == 0) {
        max_segments = 1;
    }
    init_out->max_pages = filestore_min_u32(max_segments, max_xfer_size / 4096);
    if (init_out->max_pages == 0) {
        init_out->max_pages = 1;
    }

    init_out->time_gran = 1;

    requested_flags = init_in->flags;
    if (init_in->flags & FUSE_INIT_EXT) {
        requested_flags |= ((uint64_t)init_in->flags2) << 32;
    }

    supported_flags = FUSE_ASYNC_READ | FUSE_BIG_WRITES | FUSE_MAX_PAGES | FUSE_INIT_EXT;
    accepted_flags = requested_flags & supported_flags;

    init_out->flags = (uint32_t)accepted_flags;
    init_out->flags2 = (uint32_t)(accepted_flags >> 32);

    out_hdr->len += sizeof(*init_out);
    return 0;
}

static int
filestore_op_lookup(struct spdk_fsdev_io *fsdev_io)
{
    char name[256];
    int rc;

    if (fsdev_io->u_in.fuse.hdr->nodeid != FILESTORE_ROOT_INO) {
        return -ENOENT;
    }

    rc = filestore_copy_name(fsdev_io, name, sizeof(name));
    if (rc != 0) {
        return rc;
    }

    if (strcmp(name, g_hello_name) != 0) {
        return -ENOENT;
    }

    filestore_fill_entry_out(FILESTORE_HELLO_INO, fsdev_io->u_out.fuse.op.entry);
    fsdev_io->u_out.fuse.hdr->len += sizeof(struct fuse_entry_out);
    return 0;
}

static int
filestore_op_getattr(struct spdk_fsdev_io *fsdev_io)
{
    uint64_t ino = fsdev_io->u_in.fuse.hdr->nodeid;

    if (!filestore_node_exists(ino)) {
        return -ENOENT;
    }

    filestore_fill_attr_out(ino, fsdev_io->u_out.fuse.op.attr);
    fsdev_io->u_out.fuse.hdr->len += sizeof(struct fuse_attr_out);
    return 0;
}

static int
filestore_op_open(struct spdk_fsdev_io *fsdev_io)
{
    const struct fuse_open_in *open_in = fsdev_io->u_in.fuse.op.open;
    struct fuse_open_out *open_out = fsdev_io->u_out.fuse.op.open;
    uint64_t ino = fsdev_io->u_in.fuse.hdr->nodeid;

    if (ino != FILESTORE_HELLO_INO) {
        return -EISDIR;
    }

    if (open_in != NULL && (open_in->flags & O_ACCMODE) != O_RDONLY) {
        return -EROFS;
    }

    memset(open_out, 0, sizeof(*open_out));
    open_out->fh = FILESTORE_HELLO_FH;
    fsdev_io->u_out.fuse.hdr->len += sizeof(*open_out);
    return 0;
}

static int
filestore_op_opendir(struct spdk_fsdev_io *fsdev_io)
{
    struct fuse_open_out *open_out = fsdev_io->u_out.fuse.op.open;

    if (fsdev_io->u_in.fuse.hdr->nodeid != FILESTORE_ROOT_INO) {
        return -ENOTDIR;
    }

    memset(open_out, 0, sizeof(*open_out));
    open_out->fh = FILESTORE_ROOT_FH;
    fsdev_io->u_out.fuse.hdr->len += sizeof(*open_out);
    return 0;
}

static int
filestore_fill_dirent(
    char *buf,
    size_t buf_size,
    uint64_t ino,
    uint64_t off,
    uint32_t type,
    const char *name)
{
    struct fuse_dirent *dirent;
    size_t name_len = strlen(name);
    size_t entry_len = FUSE_NAME_OFFSET + name_len;
    size_t padded_len = FUSE_DIRENT_ALIGN(entry_len);

    if (padded_len > buf_size) {
        return -EAGAIN;
    }

    dirent = (struct fuse_dirent *)buf;
    dirent->ino = ino;
    dirent->off = off;
    dirent->namelen = name_len;
    dirent->type = type;
    memcpy(dirent->name, name, name_len);
    memset(dirent->name + name_len, 0, padded_len - entry_len);

    return (int)padded_len;
}

static int
filestore_op_readdir(struct spdk_fsdev_io *fsdev_io)
{
    const struct fuse_read_in *read_in = fsdev_io->u_in.fuse.op.read;
    char *buf;
    size_t buf_size;
    uint32_t bytes_written = 0;
    uint64_t entry;

    static const struct {
        uint64_t ino;
        uint32_t type;
        const char *name;
    } entries[] = {
        {FILESTORE_ROOT_INO, DT_DIR, "."},
        {FILESTORE_ROOT_INO, DT_DIR, ".."},
        {FILESTORE_HELLO_INO, DT_REG, g_hello_name},
    };

    if (fsdev_io->u_in.fuse.hdr->nodeid != FILESTORE_ROOT_INO) {
        return -ENOTDIR;
    }

    if (read_in == NULL || fsdev_io->u_out.fuse.iovcnt <= 0 ||
        fsdev_io->u_out.fuse.iov == NULL) {
        return -EINVAL;
    }

    buf = fsdev_io->u_out.fuse.iov[0].iov_base;
    buf_size = fsdev_io->u_out.fuse.iov[0].iov_len;
    if (buf_size > read_in->size) {
        buf_size = read_in->size;
    }

    for (entry = read_in->offset; entry < SPDK_COUNTOF(entries); ++entry) {
        int rc = filestore_fill_dirent(
            buf,
            buf_size,
            entries[entry].ino,
            entry + 1,
            entries[entry].type,
            entries[entry].name);

        if (rc == -EAGAIN) {
            break;
        }
        if (rc < 0) {
            return rc;
        }

        buf += rc;
        buf_size -= rc;
        bytes_written += rc;
    }

    fsdev_io->u_out.fuse.hdr->len += bytes_written;
    return 0;
}

static int
filestore_op_read(struct spdk_fsdev_io *fsdev_io)
{
    const struct fuse_read_in *read_in = fsdev_io->u_in.fuse.op.read;
    size_t data_size = sizeof(g_hello_data) - 1;
    size_t out_size;
    size_t bytes_to_read;

    if (fsdev_io->u_in.fuse.hdr->nodeid != FILESTORE_HELLO_INO) {
        return -EISDIR;
    }

    if (read_in == NULL || read_in->fh != FILESTORE_HELLO_FH) {
        return -EINVAL;
    }

    if (fsdev_io->u_out.fuse.memory_domain != NULL) {
        return -ENOTSUP;
    }

    if (fsdev_io->u_out.fuse.iov == NULL || fsdev_io->u_out.fuse.iovcnt <= 0) {
        return -EINVAL;
    }

    if (read_in->offset >= data_size) {
        return 0;
    }

    bytes_to_read = data_size - read_in->offset;
    if (bytes_to_read > read_in->size) {
        bytes_to_read = read_in->size;
    }

    out_size = spdk_iov_length(fsdev_io->u_out.fuse.iov, fsdev_io->u_out.fuse.iovcnt);
    if (bytes_to_read > out_size) {
        bytes_to_read = out_size;
    }

    spdk_copy_buf_to_iovs(
        fsdev_io->u_out.fuse.iov,
        fsdev_io->u_out.fuse.iovcnt,
        (void *)(g_hello_data + read_in->offset),
        bytes_to_read);
    fsdev_io->u_out.fuse.hdr->len += bytes_to_read;

    return 0;
}

static int
filestore_op_statfs(struct spdk_fsdev_io *fsdev_io)
{
    struct fuse_statfs_out *statfs_out = fsdev_io->u_out.fuse.op.statfs;

    memset(statfs_out, 0, sizeof(*statfs_out));
    statfs_out->st.blocks = 1;
    statfs_out->st.bfree = 0;
    statfs_out->st.bavail = 0;
    statfs_out->st.files = 2;
    statfs_out->st.ffree = 0;
    statfs_out->st.bsize = 4096;
    statfs_out->st.frsize = 4096;
    statfs_out->st.namelen = 255;

    fsdev_io->u_out.fuse.hdr->len += sizeof(*statfs_out);
    return 0;
}

static int
filestore_op_access(struct spdk_fsdev_io *fsdev_io)
{
    const struct fuse_access_in *access_in = fsdev_io->u_in.fuse.op.access;
    uint64_t ino = fsdev_io->u_in.fuse.hdr->nodeid;

    if (!filestore_node_exists(ino)) {
        return -ENOENT;
    }

    if (access_in != NULL && (access_in->mask & W_OK)) {
        return -EACCES;
    }

    return 0;
}

static int
filestore_op_write(struct spdk_fsdev_io *fsdev_io)
{
    struct fuse_write_out *write_out = fsdev_io->u_out.fuse.op.write;

    if (write_out != NULL) {
        memset(write_out, 0, sizeof(*write_out));
    }

    return -EROFS;
}

static int
filestore_op_fuse(struct spdk_fsdev_io *fsdev_io)
{
    const struct fuse_in_header *in_hdr = fsdev_io->u_in.fuse.hdr;

    filestore_init_out_header(fsdev_io);

    switch (in_hdr->opcode) {
    case FUSE_INIT:
        return filestore_op_init(fsdev_io);
    case FUSE_DESTROY:
    case FUSE_FORGET:
    case FUSE_BATCH_FORGET:
    case FUSE_INTERRUPT:
        return 0;
    case FUSE_LOOKUP:
        return filestore_op_lookup(fsdev_io);
    case FUSE_GETATTR:
        return filestore_op_getattr(fsdev_io);
    case FUSE_OPEN:
        return filestore_op_open(fsdev_io);
    case FUSE_OPENDIR:
        return filestore_op_opendir(fsdev_io);
    case FUSE_READDIR:
        return filestore_op_readdir(fsdev_io);
    case FUSE_READ:
        return filestore_op_read(fsdev_io);
    case FUSE_RELEASE:
    case FUSE_RELEASEDIR:
    case FUSE_FLUSH:
    case FUSE_FSYNC:
    case FUSE_FSYNCDIR:
    case FUSE_SYNCFS:
        return 0;
    case FUSE_STATFS:
        return filestore_op_statfs(fsdev_io);
    case FUSE_ACCESS:
        return filestore_op_access(fsdev_io);
    case FUSE_WRITE:
        return filestore_op_write(fsdev_io);
    case FUSE_SETATTR:
    case FUSE_CREATE:
    case FUSE_MKNOD:
    case FUSE_MKDIR:
    case FUSE_UNLINK:
    case FUSE_RMDIR:
    case FUSE_RENAME:
    case FUSE_RENAME2:
    case FUSE_LINK:
    case FUSE_SYMLINK:
    case FUSE_FALLOCATE:
    case FUSE_COPY_FILE_RANGE:
    case FUSE_COPY_FILE_RANGE_64:
    case FUSE_TMPFILE:
    case FUSE_SETXATTR:
    case FUSE_REMOVEXATTR:
        return -EROFS;
    case FUSE_GETXATTR:
    case FUSE_LISTXATTR:
    case FUSE_READLINK:
    case FUSE_READDIRPLUS:
    case FUSE_GETLK:
    case FUSE_SETLK:
    case FUSE_SETLKW:
    case FUSE_IOCTL:
    case FUSE_POLL:
    case FUSE_BMAP:
    case FUSE_SETUPMAPPING:
    case FUSE_REMOVEMAPPING:
    case FUSE_LSEEK:
    default:
        return -ENOSYS;
    }
}

static int
filestore_destruct(void *ctx)
{
    struct filestore_fsdev *fs = ctx;

    free(fs->fsdev.name);
    free(fs);
    return 0;
}

static void
filestore_submit_request(struct spdk_io_channel *ch, struct spdk_fsdev_io *fsdev_io)
{
    int status;

    (void)ch;

    status = filestore_op_fuse(fsdev_io);
    filestore_io_complete(fsdev_io, status);
}

static struct spdk_io_channel *
filestore_get_io_channel(void *ctx)
{
    (void)ctx;
    return spdk_get_io_channel(&g_filestore_io_device);
}

static void
filestore_write_config_json(struct spdk_fsdev *fsdev, struct spdk_json_write_ctx *w)
{
    spdk_json_write_object_begin(w);
    spdk_json_write_named_string(w, "method", "fsdev_filestore_create");
    spdk_json_write_named_object_begin(w, "params");
    spdk_json_write_named_string(w, "name", spdk_fsdev_get_name(fsdev));
    spdk_json_write_object_end(w);
    spdk_json_write_object_end(w);
}

static int
filestore_dump_info_json(void *ctx, struct spdk_json_write_ctx *w)
{
    (void)ctx;
    spdk_json_write_named_string(w, "backend", "filestore-stub");
    spdk_json_write_named_string(w, "file", g_hello_name);
    spdk_json_write_named_string(w, "content", g_hello_data);
    return 0;
}

static int
filestore_set_notifications(void *ctx, bool enabled)
{
    (void)ctx;
    return enabled ? -EOPNOTSUPP : 0;
}

static const struct spdk_fsdev_fn_table g_filestore_fn_table = {
    .destruct = filestore_destruct,
    .submit_request = filestore_submit_request,
    .get_io_channel = filestore_get_io_channel,
    .write_config_json = filestore_write_config_json,
    .dump_info_json = filestore_dump_info_json,
    .set_notifications = filestore_set_notifications,
};

static int
filestore_create(struct spdk_fsdev **fsdev, const char *name)
{
    struct filestore_fsdev *fs;
    int rc;

    if (name == NULL || name[0] == '\0') {
        return -EINVAL;
    }

    fs = calloc(1, sizeof(*fs));
    if (fs == NULL) {
        return -ENOMEM;
    }

    fs->fsdev.name = strdup(name);
    if (fs->fsdev.name == NULL) {
        free(fs);
        return -ENOMEM;
    }

    fs->fsdev.ctxt = fs;
    fs->fsdev.module = &g_filestore_module;
    fs->fsdev.fn_table = &g_filestore_fn_table;

    rc = spdk_fsdev_register(&fs->fsdev);
    if (rc != 0) {
        filestore_destruct(fs);
        return rc;
    }

    *fsdev = &fs->fsdev;
    return 0;
}

static void
filestore_delete(const char *name, spdk_fsdev_unregister_cb cb_fn, void *cb_arg)
{
    int rc = spdk_fsdev_unregister_by_name(name, &g_filestore_module, cb_fn, cb_arg);
    if (rc != 0 && cb_fn != NULL) {
        cb_fn(cb_arg, rc);
    }
}

struct rpc_filestore_create {
    char *name;
};

static const struct spdk_json_object_decoder g_rpc_filestore_create_decoders[] = {
    {"name", offsetof(struct rpc_filestore_create, name), spdk_json_decode_string, false},
};

static void
rpc_filestore_create(struct spdk_jsonrpc_request *request, const struct spdk_json_val *params)
{
    struct rpc_filestore_create req = {};
    struct spdk_json_write_ctx *w;
    struct spdk_fsdev *fsdev = NULL;
    int rc;

    if (spdk_json_decode_object(
            params,
            g_rpc_filestore_create_decoders,
            SPDK_COUNTOF(g_rpc_filestore_create_decoders),
            &req) != 0) {
        spdk_jsonrpc_send_error_response(
            request,
            SPDK_JSONRPC_ERROR_INVALID_PARAMS,
            "spdk_json_decode_object failed");
        free(req.name);
        return;
    }

    rc = filestore_create(&fsdev, req.name);
    if (rc != 0) {
        spdk_jsonrpc_send_error_response(
            request,
            SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
            strerror(-rc));
        free(req.name);
        return;
    }

    w = spdk_jsonrpc_begin_result(request);
    if (w == NULL) {
        free(req.name);
        return;
    }
    spdk_json_write_string(w, spdk_fsdev_get_name(fsdev));
    spdk_jsonrpc_end_result(request, w);

    free(req.name);
}
SPDK_RPC_REGISTER("fsdev_filestore_create", rpc_filestore_create, SPDK_RPC_RUNTIME)

struct rpc_filestore_delete {
    char *name;
};

static const struct spdk_json_object_decoder g_rpc_filestore_delete_decoders[] = {
    {"name", offsetof(struct rpc_filestore_delete, name), spdk_json_decode_string, false},
};

static void
rpc_filestore_delete_cb(void *cb_arg, int fsdeverrno)
{
    struct spdk_jsonrpc_request *request = cb_arg;

    if (fsdeverrno == 0) {
        spdk_jsonrpc_send_bool_response(request, true);
    } else {
        spdk_jsonrpc_send_error_response(
            request,
            SPDK_JSONRPC_ERROR_INTERNAL_ERROR,
            strerror(-fsdeverrno));
    }
}

static void
rpc_filestore_delete(struct spdk_jsonrpc_request *request, const struct spdk_json_val *params)
{
    struct rpc_filestore_delete req = {};

    if (spdk_json_decode_object(
            params,
            g_rpc_filestore_delete_decoders,
            SPDK_COUNTOF(g_rpc_filestore_delete_decoders),
            &req) != 0) {
        spdk_jsonrpc_send_error_response(
            request,
            SPDK_JSONRPC_ERROR_INVALID_PARAMS,
            "spdk_json_decode_object failed");
        free(req.name);
        return;
    }

    filestore_delete(req.name, rpc_filestore_delete_cb, request);
    free(req.name);
}
SPDK_RPC_REGISTER("fsdev_filestore_delete", rpc_filestore_delete, SPDK_RPC_RUNTIME)

static struct spdk_fsdev_module g_filestore_module = {
    .name = "filestore",
    .module_init = filestore_module_init,
    .module_fini = filestore_module_fini,
    .get_ctx_size = filestore_get_ctx_size,
};

SPDK_FSDEV_MODULE_REGISTER(filestore, &g_filestore_module);
