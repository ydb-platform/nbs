#pragma once

#include "public.h"

#include "request.h"

#include <cloud/storage/core/libs/common/context.h>
#include <cloud/storage/core/libs/common/error.h>

#include <atomic>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct TCallContext final: public TCallContextBase
{
public:
    TString FileSystemId;

    EFileStoreRequest RequestType = EFileStoreRequest::MAX;
    ui64 RequestSize = 0;
    bool Unaligned = false;

    int CancellationCode = 0;
    std::atomic<bool> Cancelled = false;

    explicit TCallContext(ui64 requestId = 0);
    TCallContext(TString fileSystemId, ui64 requestId = 0);

    TString LogString() const;
};

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_TRACK(probe, context, type, ...)                             \
    LWTRACK(probe, context->LWOrbit, type, context->RequestId, ##__VA_ARGS__); \
    // FILESTORE_TRACK

}   // namespace NCloud::NFileStore
