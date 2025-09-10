#pragma once

#if defined(__cplusplus)
extern "C" {
#endif

#if defined(FUSE_VIRTIO)
#   include <contrib/libs/virtiofsd/fuse_lowlevel.h>
#else
#   include <contrib/libs/fuse/include/fuse_lowlevel.h>
#endif

uint64_t fuse_req_unique(fuse_req_t req);

struct fuse_session_params {
    uint32_t proto_major;
    uint32_t proto_minor;
    uint32_t capable;
    uint32_t want;
    uint32_t bufsize;
};

void fuse_session_setparams(
    struct fuse_session* se,
    const struct fuse_session_params* params);

void fuse_session_getparams(
    struct fuse_session* se,
    struct fuse_session_params* params);

enum fuse_cancelation_code {
    FUSE_ERROR = 0,
    FUSE_SUSPEND = 1,
};

int fuse_cancel_request(
    fuse_req_t req,
    enum fuse_cancelation_code code);

// 'overrides' fuse_reply_none
void fuse_reply_none_override(fuse_req_t req);

#if defined(__cplusplus)
}   // extern "C"
#endif
