#include "switchable_client.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/vector.h>
#include <util/system/spinlock.h>

#include <utility>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
concept HasSetDiskId = requires(T& obj) {
    { obj.SetDiskId(TString()) } -> std::same_as<void>;
};

template <typename T>
void SetDiskIdIfExists(T& obj, const TString& diskId)
{
    if constexpr (HasSetDiskId<T>) {
        obj.SetDiskId(diskId);
    }
}

template <typename T>
concept HasSetSessionId = requires(T& obj) {
    { obj.SetSessionId(TString()) } -> std::same_as<void>;
};

template <typename T>
void SetSessionIdIfExists(T& obj, const TString& sessionId)
{
    if constexpr (HasSetSessionId<T>) {
        obj.SetSessionId(sessionId);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TBlockStoreAdapter
{
public:
    static auto Execute(
        IBlockStore* blockStore,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksRequest> request)
    {
        return blockStore->ReadBlocks(
            std::move(callContext),
            std::move(request));
    }

    static auto Execute(
        IBlockStore* blockStore,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
    {
        return blockStore->ReadBlocksLocal(
            std::move(callContext),
            std::move(request));
    }

    static auto Execute(
        IBlockStore* blockStore,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksRequest> request)
    {
        return blockStore->WriteBlocks(
            std::move(callContext),
            std::move(request));
    }

    static auto Execute(
        IBlockStore* blockStore,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
    {
        return blockStore->WriteBlocksLocal(
            std::move(callContext),
            std::move(request));
    }

    static auto Execute(
        IBlockStore* blockStore,
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request)
    {
        return blockStore->ZeroBlocks(
            std::move(callContext),
            std::move(request));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest, typename TResponse>
class TRequestsHolder
{
private:
    struct TRequestInfo
    {
        TPromise<TResponse> Promise;
        TCallContextPtr CallContext;
        std::shared_ptr<TRequest> Request;
    };

    TAdaptiveLock RequestsLock;
    TVector<TRequestInfo> Requests;

public:
    TFuture<TResponse> SaveRequest(
        TCallContextPtr callContext,
        std::shared_ptr<TRequest> request)
    {
        with_lock (RequestsLock) {
            Requests.emplace_back(
                TRequestInfo{
                    .Promise = NewPromise<TResponse>(),
                    .CallContext = std::move(callContext),
                    .Request = std::move(request)});
            return Requests.back().Promise;
        }
    }

    void ExecuteSavedRequests(
        IBlockStore* blockStore,
        const TString& diskId,
        const TString& sessionId)
    {
        TVector<TRequestInfo> requests;
        with_lock (RequestsLock) {
            requests.swap(Requests);
        }

        for (auto& requestInfo: requests) {
            if (diskId) {
                requestInfo.Request->SetDiskId(diskId);
            }
            if (sessionId) {
                requestInfo.Request->SetSessionId(sessionId);
            }

            auto future = TBlockStoreAdapter::Execute(
                blockStore,
                std::move(requestInfo.CallContext),
                std::move(requestInfo.Request));
            future.Subscribe(
                [promise = std::move(requestInfo.Promise)](
                    TFuture<TResponse> f) mutable
                {
                    promise.SetValue(f.ExtractValue());   //
                });
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSwitchableBlockStore final
    : public std::enable_shared_from_this<TSwitchableBlockStore>
    , public ISwitchableBlockStore
{
private:
    TLog Log;

    struct TClientInfo
    {
        IBlockStorePtr Client;
        TString DiskId;
        TString SessionId;
    };

    TClientInfo PrimaryClientInfo;
    TClientInfo SecondaryClientInfo;

    // BeforeSwitching() sets the WillSwitchToSecondary to true. After that,
    // all data-plane requests are saved and not sent for execution. Calling
    // AfterSwitching() sets WillSwitchToSecondary to false and sends all saved
    // requests for execution.
    std::atomic_bool WillSwitchToSecondary{false};

    // Switch() sets the SwitchedToSecondary to true. After that, all data-plane
    // requests executed with client from SecondaryClientInfo.
    // Reverse switching is not possible.
    std::atomic_bool SwitchedToSecondary{false};

    TRequestsHolder<NProto::TReadBlocksRequest, NProto::TReadBlocksResponse>
        ReadBlocksRequests;
    TRequestsHolder<
        NProto::TReadBlocksLocalRequest,
        NProto::TReadBlocksLocalResponse>
        ReadBlocksLocalRequests;
    TRequestsHolder<NProto::TWriteBlocksRequest, NProto::TWriteBlocksResponse>
        WriteBlocksRequests;
    TRequestsHolder<
        NProto::TWriteBlocksLocalRequest,
        NProto::TWriteBlocksLocalResponse>
        WriteBlocksLocalRequests;
    TRequestsHolder<NProto::TZeroBlocksRequest, NProto::TZeroBlocksResponse>
        ZeroBlocksRequests;

public:
    TSwitchableBlockStore(
        ILoggingServicePtr logging,
        TString diskId,
        IBlockStorePtr client)
        : Log(logging->CreateLog("BLOCKSTORE_CLIENT"))
        , PrimaryClientInfo(
              {.Client = std::move(client),
               .DiskId = std::move(diskId),
               .SessionId = {}})
    {}

    void BeforeSwitching() override
    {
        Y_ABORT_UNLESS(!WillSwitchToSecondary);
        STORAGE_INFO("Will switch from " << PrimaryClientInfo.DiskId.Quote());
        WillSwitchToSecondary = true;
    }

    void Switch(
        IBlockStorePtr newClient,
        const TString& newDiskId,
        const TString& newSessionId) override
    {
        Y_ABORT_UNLESS(WillSwitchToSecondary);
        Y_ABORT_UNLESS(!SwitchedToSecondary);

        STORAGE_INFO(
            "Switched from " << PrimaryClientInfo.DiskId.Quote() << " to "
                             << newDiskId.Quote());

        SecondaryClientInfo = {
            .Client = std::move(newClient),
            .DiskId = newDiskId,
            .SessionId = newSessionId};
        SwitchedToSecondary = true;
    }

    void AfterSwitching() override
    {
        Y_ABORT_UNLESS(WillSwitchToSecondary);
        WillSwitchToSecondary = false;

        if (SwitchedToSecondary) {
            STORAGE_INFO(
                "Switching from " << PrimaryClientInfo.DiskId.Quote() << " to "
                                  << SecondaryClientInfo.DiskId.Quote()
                                  << " is completed");
        } else {
            STORAGE_INFO(
                "Switching from " << PrimaryClientInfo.DiskId.Quote()
                                  << " is interrupted");
        }

        const TClientInfo& currentClientInfo =
            SwitchedToSecondary ? SecondaryClientInfo : PrimaryClientInfo;

        ReadBlocksRequests.ExecuteSavedRequests(
            currentClientInfo.Client.get(),
            currentClientInfo.DiskId,
            currentClientInfo.SessionId);
        ReadBlocksLocalRequests.ExecuteSavedRequests(
            currentClientInfo.Client.get(),
            currentClientInfo.DiskId,
            currentClientInfo.SessionId);
        WriteBlocksRequests.ExecuteSavedRequests(
            currentClientInfo.Client.get(),
            currentClientInfo.DiskId,
            currentClientInfo.SessionId);
        WriteBlocksLocalRequests.ExecuteSavedRequests(
            currentClientInfo.Client.get(),
            currentClientInfo.DiskId,
            currentClientInfo.SessionId);
        ZeroBlocksRequests.ExecuteSavedRequests(
            currentClientInfo.Client.get(),
            currentClientInfo.DiskId,
            currentClientInfo.SessionId);
    }

    void Start() override
    {
        PrimaryClientInfo.Client->Start();
    }

    void Stop() override
    {
        PrimaryClientInfo.Client->Stop();
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return PrimaryClientInfo.Client->AllocateBuffer(bytesCount);
    }

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        constexpr bool isSwitchableRequest = IsReadWriteRequest(               \
            GetBlockStoreRequest<NProto::T##name##Request>());                 \
        if constexpr (isSwitchableRequest) {                                   \
            if (SwitchedToSecondary) {                                         \
                STORAGE_TRACE(                                                 \
                    "Forward " << #name << " from "                            \
                               << PrimaryClientInfo.DiskId.Quote() << " to "   \
                               << SecondaryClientInfo.DiskId.Quote());         \
                SetDiskIdIfExists(*request, SecondaryClientInfo.DiskId);       \
                SetSessionIdIfExists(*request, SecondaryClientInfo.SessionId); \
                return SecondaryClientInfo.Client->name(                       \
                    std::move(callContext),                                    \
                    std::move(request));                                       \
            }                                                                  \
            if (WillSwitchToSecondary) {                                       \
                STORAGE_TRACE("Save " << #name);                               \
                return SaveRequest<NProto::T##name##Response>(                 \
                    std::move(callContext),                                    \
                    std::move(request));                                       \
            }                                                                  \
        }                                                                      \
        return PrimaryClientInfo.Client->name(                                 \
            std::move(callContext),                                            \
            std::move(request));                                               \
    }

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

private:
    template <typename U, typename T>
    TFuture<U> SaveRequest(
        TCallContextPtr callContext,
        std::shared_ptr<T> request);

    template <>
    TFuture<NProto::TReadBlocksResponse> SaveRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksRequest> request)
    {
        return ReadBlocksRequests.SaveRequest(
            std::move(callContext),
            std::move(request));
    }

    template <>
    TFuture<NProto::TReadBlocksLocalResponse> SaveRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadBlocksLocalRequest> request)
    {
        return ReadBlocksLocalRequests.SaveRequest(
            std::move(callContext),
            std::move(request));
    }

    template <>
    TFuture<NProto::TWriteBlocksResponse> SaveRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksRequest> request)
    {
        return WriteBlocksRequests.SaveRequest(
            std::move(callContext),
            std::move(request));
    }
    template <>
    TFuture<NProto::TWriteBlocksLocalResponse> SaveRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteBlocksLocalRequest> request)
    {
        return WriteBlocksLocalRequests.SaveRequest(
            std::move(callContext),
            std::move(request));
    }

    template <>
    TFuture<NProto::TZeroBlocksResponse> SaveRequest(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TZeroBlocksRequest> request)
    {
        return ZeroBlocksRequests.SaveRequest(
            std::move(callContext),
            std::move(request));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSessionSwitchingGuard final: public ISessionSwitchingGuard
{
    ISwitchableBlockStorePtr SwitchableDataClient;

public:
    explicit TSessionSwitchingGuard(
        ISwitchableBlockStorePtr switchableDataClient)
        : SwitchableDataClient(std::move(switchableDataClient))
    {
        SwitchableDataClient->BeforeSwitching();
    }

    ~TSessionSwitchingGuard() override
    {
        SwitchableDataClient->AfterSwitching();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ISwitchableBlockStorePtr CreateSwitchableClient(
    ILoggingServicePtr logging,
    TString diskId,
    IBlockStorePtr client)
{
    return std::make_shared<TSwitchableBlockStore>(
        std::move(logging),
        std::move(diskId),
        std::move(client));
}

ISessionSwitchingGuardPtr CreateSessionSwitchingGuard(
    ISwitchableBlockStorePtr switchableDataClient)
{
    return std::make_shared<TSessionSwitchingGuard>(
        std::move(switchableDataClient));
}

}   // namespace NCloud::NBlockStore
