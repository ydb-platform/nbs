#include "fuse.h"

#include <contrib/libs/fuse/lib/fuse_i.h>

#include <errno.h>

uint64_t fuse_req_unique(fuse_req_t req)
{
    return req->unique;
}

int fuse_cancel_request(
    fuse_req_t req,
    enum fuse_cancelation_code code)
{
    (void)code;
    return fuse_reply_err(req, EINTR);
}

void fuse_reply_none_override(fuse_req_t req)
{
    fuse_reply_none(req);
}
