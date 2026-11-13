#pragma once

#if defined(__cplusplus)
extern "C" {
#endif

#if defined(FUSE_VIRTIO)
#   include <cloud/contrib/virtiofsd/fuse_lowlevel.h>
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

// Raises the session buffer size so a future FUSE_INIT can offer up to the
// given max_write. Never shrinks: a restored (already negotiated) size must
// keep being accepted.
void fuse_session_update_bufsize(struct fuse_session* se, uint32_t max_write);

// Converts a session buffer size (fuse_session_params.bufsize) to the max_write
// it can carry, i.e. strips the FUSE request header room.
uint32_t fuse_bufsize_to_max_write(uint32_t bufsize);

// Returns the session buffer size FUSE_INIT ends up with once max_write is
// negotiated: min(bufsize, max_write + header).
uint32_t fuse_negotiated_bufsize(uint32_t bufsize, uint32_t max_write);

enum fuse_cancelation_code {
    FUSE_ERROR = 0,
    FUSE_SUSPEND = 1,
};

int fuse_cancel_request(
    fuse_req_t req,
    enum fuse_cancelation_code code);

#if defined(__cplusplus)
}   // extern "C"
#endif
