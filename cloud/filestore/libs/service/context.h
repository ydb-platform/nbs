#pragma once

#include "public.h"
#include "request.h"

#include <cloud/storage/core/libs/common/context.h>
#include <cloud/storage/core/libs/common/error.h>

#include <atomic>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct TCallContext final
    : public TCallContextBase
{
public:
    TString FileSystemId;

    EFileStoreRequest RequestType = EFileStoreRequest::MAX;

    // The FUSE request type as accounted by the per-client availability
    // metric (None for requests outside the availability SLA). Assigned at
    // the FUSE dispatch together with RequestType.
    EFileStoreAvailabilityRequestType AvailabilityRequestType =
        EFileStoreAvailabilityRequestType::None;

    ui64 RequestSize = 0;
    bool Unaligned = false;

    ui64 LoopThreadId = 0;

    int CancellationCode = 0;
    std::atomic<bool> Cancelled = false;

    // The errno sent to the guest in the response: the error code passed to
    // fuse_reply_err() or 0 for successful replies and cancelled requests.
    // Set by the vfs_fuse layer right before reporting request completion
    // and reset by TAvailabilityCounters when the request is (re)registered.
    // Used by the per-client availability metric to classify terminal request
    // outcomes (EIO vs any other outcome), because the internal request error
    // does not always match the guest-visible outcome.
    int GuestReplyErrno = 0;

    // Availability registration stamp, maintained by TAvailabilityCounters
    // (see request stats): 0 means the request is not registered with the
    // availability metric; otherwise the sequence number of the availability
    // interval the request started in, plus one. Consumed (reset to 0) when
    // the completion is reported.
    ui64 AvailabilityIntervalSeqNo = 0;

    explicit TCallContext(ui64 requestId = 0);
    explicit TCallContext(TString fileSystemId, ui64 requestId = 0);

    TString LogString() const;
};

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_TRACK(probe, context, type, ...)                             \
    LWTRACK(                                                                   \
        probe,                                                                 \
        context->LWOrbit,                                                      \
        type,                                                                  \
        context->RequestId,                                                    \
        ##__VA_ARGS__);                                                        \
// FILESTORE_TRACK

}   // namespace NCloud::NFileStore
