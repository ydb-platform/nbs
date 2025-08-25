#include "switchable_session.h"

#include "session.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/ptr.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TSwitchableSession final
    : public std::enable_shared_from_this<TSwitchableSession>
    , public ISwitchableSession
{
private:
    ISessionPtr Session;

public:
    explicit TSwitchableSession(ISessionPtr session)
        : Session(std::move(session))
    {}

    ui32 GetMaxTransfer() const override
    {
        return Session->GetMaxTransfer();
    }

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        NProto::EVolumeAccessMode accessMode,
        NProto::EVolumeMountMode mountMode,
        ui64 mountSeqNumber,
        TCallContextPtr callContext,
        const NProto::THeaders& headers) override
    {
        return Session->MountVolume(
            accessMode,
            mountMode,
            mountSeqNumber,
            std::move(callContext),
            headers);
    }

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr callContext,
        const NProto::THeaders& headers) override
    {
        return Session->MountVolume(std::move(callContext), headers);
    }

    TFuture<NProto::TUnmountVolumeResponse> UnmountVolume(
        TCallContextPtr callContext,
        const NProto::THeaders& headers) override
    {
        return Session->UnmountVolume(std::move(callContext), headers);
    }

    TFuture<NProto::TMountVolumeResponse> EnsureVolumeMounted() override
    {
        return Session->EnsureVolumeMounted();
    }

    void SwitchSession(ISessionPtr newSession) override
    {
        Session = std::move(newSession);
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        return Session->ReadBlocksLocal(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        return Session->WriteBlocksLocal(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        return Session->ZeroBlocks(std::move(callContext), std::move(request));
    }

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        return Session->EraseDevice(method);
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Session->AllocateBuffer(bytesCount);
    }

    void ReportIOError() override
    {
        Session->ReportIOError();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ISwitchableSessionPtr CreateSwitchableSession(ISessionPtr session)
{
    return std::make_shared<TSwitchableSession>(std::move(session));
}

}   // namespace NCloud::NBlockStore::NClient
