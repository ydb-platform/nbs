#pragma once

#include "public.h"

#include "protocol.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/guarded_sglist.h>
#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/ptr.h>
#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

enum class EClientRequestType
{
    ReadBlocks,
    WriteBlocks,
    ZeroBlocks,
    MountVolume,
};

////////////////////////////////////////////////////////////////////////////////

struct TClientRequest
    : public TAtomicRefCount<TClientRequest>
{
    const EClientRequestType RequestType;
    const ui64 RequestId;
    const ui64 BlockIndex;
    const ui32 BlocksCount;
    const TGuardedSgList SgList;

    TClientRequest(
        EClientRequestType requestType,
        ui64 requestId,
        ui64 blockIndex,
        ui32 blocksCount,
        TGuardedSgList sglist = {});

    virtual ~TClientRequest() = default;

    virtual void Complete(const NProto::TError& error) = 0;
};

using TClientRequestPtr = TIntrusivePtr<TClientRequest>;

////////////////////////////////////////////////////////////////////////////////

struct IClientHandler
{
    virtual ~IClientHandler() = default;

    virtual const TExportInfo& GetExportInfo() = 0;

    virtual bool NegotiateClient(
        IInputStream& in,
        IOutputStream& out) = 0;

    virtual void SendRequest(
        IOutputStream& out,
        TClientRequestPtr request) = 0;

    virtual void ProcessRequests(IInputStream& in) = 0;

    virtual void CancelAllRequests(const NProto::TError& error) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IClientHandlerPtr CreateClientHandler(
    ILoggingServicePtr logging,
    bool structuredReply = false,
    bool useNbsErrors = false);

////////////////////////////////////////////////////////////////////////////////

inline ui32 GetBlockSize(const TExportInfo& exportInfo)
{
    return exportInfo.OptBlockSize;
}

}   // namespace NCloud::NBlockStore::NBD
