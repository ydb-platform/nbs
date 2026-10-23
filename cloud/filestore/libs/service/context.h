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

    ui64 RequestSize = 0;
    bool Unaligned = false;

    ui64 LoopThreadId = 0;

    int CancellationCode = 0;
    std::atomic<bool> Cancelled = false;

    // The errno sent to the guest in the response.
    // Should be set by right before reporting request completion.
    int GuestReplyErrno = 0;

    // Availability registration stamp, maintained by TAvailabilityCounters
    // (see request stats): 0 means the request is not registered with the
    // availability metric.
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
