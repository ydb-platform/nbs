#include "switchable_session.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/vector.h>
#include <util/system/mutex.h>

#include <utility>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct ISwitchableSessionPrivate
{
    virtual ~ISwitchableSessionPrivate() = default;

    virtual void OnRequestFinished() = 0;
    [[nodiscard]] virtual ISession* GetSession() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TReadBlocksLocalResponse> ExecuteRequest(
    ISession* session,
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
{
    return session->ReadBlocksLocal(std::move(callContext), std::move(request));
}

TFuture<NProto::TWriteBlocksLocalResponse> ExecuteRequest(
    ISession* session,
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
{
    return session->WriteBlocksLocal(
        std::move(callContext),
        std::move(request));
}

TFuture<NProto::TZeroBlocksResponse> ExecuteRequest(
    ISession* session,
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TZeroBlocksRequest> request)
{
    return session->ZeroBlocks(std::move(callContext), std::move(request));
}

TFuture<NProto::TError> ExecuteRequest(
    ISession* session,
    TCallContextPtr callContext,
    std::shared_ptr<NProto::EDeviceEraseMethod> request)
{
    Y_UNUSED(callContext);

    return session->EraseDevice(*request);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest, typename TResponse>
class TDelayedRequestQueue
{
    using TRequestPtr = std::shared_ptr<TRequest>;
    using TPromise = TPromise<TResponse>;
    using TFuture = TFuture<TResponse>;

    struct TRequestAndPromise
    {
        TCallContextPtr CallContext;
        TRequestPtr Request;
        TPromise Promise;
    };

    bool& Draining;
    size_t& InflightRequestCount;
    TMutex& DrainLock;
    TVector<TRequestAndPromise> DelayedRequests;

public:
    TDelayedRequestQueue(
        bool& draining,
        size_t& inflightRequestCount,
        TMutex& drainLock)
        : Draining(draining)
        , InflightRequestCount(inflightRequestCount)
        , DrainLock(drainLock)
    {}

    TFuture ExecuteOrDelay(
        std::shared_ptr<ISwitchableSessionPrivate> parent,
        TCallContextPtr callContext,
        TRequestPtr request)
    {
        TFuture result;
        bool requestDelayed = false;
        with_lock (DrainLock) {
            if (!Draining) {
                ++InflightRequestCount;
            } else {
                requestDelayed = true;
                TRequestAndPromise requestAndPromise{
                    .CallContext = std::move(callContext),
                    .Request = std::move(request),
                    .Promise = NewPromise<TResponse>()};
                result = requestAndPromise.Promise;
                DelayedRequests.push_back(std::move(requestAndPromise));
            }
        }

        if (!requestDelayed) {
            result = ExecuteRequest(
                parent->GetSession(),
                std::move(callContext),
                std::move(request));
        }

        result.Subscribe(
            [weakParent = std::weak_ptr<ISwitchableSessionPrivate>(parent)](
                const TFuture& future)
            {
                Y_UNUSED(future);

                if (auto parent = weakParent.lock()) {
                    parent->OnRequestFinished();
                }
            });
        return result;
    }

    void ExecuteDelayed(ISession* session)
    {
        for (auto& [callContext, request, promise]: DelayedRequests) {
            ++InflightRequestCount;
            auto future = ExecuteRequest(
                session,
                std::move(callContext),
                std::move(request));

            future.Apply([promise](TFuture future) mutable
                         { promise.SetValue(future.ExtractValue()); });
        }
        DelayedRequests.clear();
    }
};

using TDelayedReadBlocks = TDelayedRequestQueue<
    NProto::TReadBlocksLocalRequest,
    NProto::TReadBlocksLocalResponse>;

using TDelayedWriteBlocks = TDelayedRequestQueue<
    NProto::TWriteBlocksLocalRequest,
    NProto::TWriteBlocksLocalResponse>;

using TDelayedZeroBlocks = TDelayedRequestQueue<
    NProto::TZeroBlocksRequest,
    NProto::TZeroBlocksResponse>;

using TDelayedErases =
    TDelayedRequestQueue<NProto::EDeviceEraseMethod, NProto::TError>;

////////////////////////////////////////////////////////////////////////////////

class TSwitchableSession final
    : public std::enable_shared_from_this<TSwitchableSession>
    , public ISwitchableSession
    , public ISwitchableSessionPrivate
{
private:
    const ILoggingServicePtr Logging;
    const TString DiskId;

    TLog Log;
    ISessionPtr Session;
    bool Draining = false;
    size_t InflightRequestCount = 0;
    TMutex DrainLock;
    TPromise<void> DrainPromise = NewPromise<void>();

    TDelayedReadBlocks DelayedReads{
        Draining,
        InflightRequestCount,
        DrainLock};
    TDelayedWriteBlocks DelayedWrites{
        Draining,
        InflightRequestCount,
        DrainLock};
    TDelayedZeroBlocks DelayedZeroes{
        Draining,
        InflightRequestCount,
        DrainLock};
    TDelayedErases DelayedErases{
        Draining,
        InflightRequestCount,
        DrainLock};

public:
    TSwitchableSession(
        ILoggingServicePtr logging,
        TString diskId,
        ISessionPtr session)
        : Logging(std::move(logging))
        , DiskId(std::move(diskId))
        , Log(Logging->CreateLog("BLOCKSTORE_CLIENT"))
        , Session(std::move(session))
    {}

    // Implement ISwitchableSessionPrivate

    void OnRequestFinished() override
    {
        bool drainFinished = false;
        with_lock (DrainLock) {
            --InflightRequestCount;
            drainFinished = Draining && InflightRequestCount == 0;
        }
        if (drainFinished) {
            OnDrainFinished();
        }
    }

    ISession* GetSession() const override
    {
        return Session.get();
    }

    // Implement ISwitchableSession

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

    NThreading::TFuture<void> Drain() override
    {
        bool drainFinished = false;
        with_lock (DrainLock) {
            STORAGE_INFO("Draining started. InflightRequestCount: " << InflightRequestCount);
            Draining = true;
            drainFinished = InflightRequestCount == 0;
        }
        if (drainFinished) {
            OnDrainFinished();
        }
        return DrainPromise;
    }

    void SwitchSession(ISessionPtr newSession) override
    {
        STORAGE_INFO("Switch " << DiskId.Quote() << " to new session");
        with_lock (DrainLock) {
            Y_DEBUG_ABORT_UNLESS(Draining);

            Draining = false;
            DrainPromise = NewPromise<void>();

            Session = std::move(newSession);

            DelayedReads.ExecuteDelayed(Session.get());
            DelayedWrites.ExecuteDelayed(Session.get());
            DelayedZeroes.ExecuteDelayed(Session.get());
            DelayedErases.ExecuteDelayed(Session.get());
        }
    }

    TFuture<NProto::TReadBlocksLocalResponse> ReadBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request) override
    {
        return DelayedReads.ExecuteOrDelay(
            shared_from_this(),
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TWriteBlocksLocalResponse> WriteBlocksLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request) override
    {
        return DelayedWrites.ExecuteOrDelay(
            shared_from_this(),
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TZeroBlocksResponse> ZeroBlocks(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request) override
    {
        return DelayedZeroes.ExecuteOrDelay(
            shared_from_this(),
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TError> EraseDevice(
        NProto::EDeviceEraseMethod method) override
    {
        TCallContextPtr ctx;
        return DelayedErases.ExecuteOrDelay(
            shared_from_this(),
            TCallContextPtr(),
            std::make_shared<NProto::EDeviceEraseMethod>(method));
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Session->AllocateBuffer(bytesCount);
    }

    void ReportIOError() override
    {
        Session->ReportIOError();
    }

private:
    void OnDrainFinished()
    {
        STORAGE_INFO("Drain for " << DiskId.Quote() << " finished");
        DrainPromise.SetValue();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ISwitchableSessionPtr CreateSwitchableSession(
    ILoggingServicePtr logging,
    TString diskId,
    ISessionPtr session)
{
    return std::make_shared<TSwitchableSession>(
        std::move(logging),
        std::move(diskId),
        std::move(session));
}

}   // namespace NCloud::NBlockStore::NClient
