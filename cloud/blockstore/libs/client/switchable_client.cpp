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
struct TClientInfo
{
    IBlockStorePtr Client;
    TString DiskId;
    TString SessionId;
};

template <typename TRequest, typename TResponse>
class TDeferredRequestsHolder
{
private:
    struct TRequestInfo
    {
        TPromise<TResponse> Promise;
        TCallContextPtr CallContext;
        std::shared_ptr<TRequest> Request;
    };

    TVector<TRequestInfo> Requests;

public:
    TFuture<TResponse> SaveRequest(
        TCallContextPtr callContext,
        std::shared_ptr<TRequest> request)
    {
        Requests.emplace_back(
            TRequestInfo{
                .Promise = NewPromise<TResponse>(),
                .CallContext = std::move(callContext),
                .Request = std::move(request)});
        return Requests.back().Promise;
    }

    void ExecuteSavedRequests(const TClientInfo& clientInfo)
    {
        Y_ABORT_UNLESS(clientInfo.Client);

        for (auto& requestInfo: Requests) {
            if (clientInfo.DiskId) {
                requestInfo.Request->SetDiskId(clientInfo.DiskId);
            }
            if (clientInfo.SessionId) {
                requestInfo.Request->SetSessionId(clientInfo.SessionId);
            }

            auto future = TBlockStoreRequestAdapter::Execute(
                clientInfo.Client.get(),
                std::move(requestInfo.CallContext),
                std::move(requestInfo.Request));
            future.Subscribe(
                [promise = std::move(requestInfo.Promise)](
                    TFuture<TResponse> f) mutable
                {
                    promise.SetValue(f.ExtractValue());   //
                });
        }
        Requests.clear();
    }
};

using TDeferredRequestsHolders = std::tuple<
    TDeferredRequestsHolder<
        NProto::TReadBlocksRequest,
        NProto::TReadBlocksResponse>,
    TDeferredRequestsHolder<
        NProto::TReadBlocksLocalRequest,
        NProto::TReadBlocksLocalResponse>,
    TDeferredRequestsHolder<
        NProto::TWriteBlocksRequest,
        NProto::TWriteBlocksResponse>,
    TDeferredRequestsHolder<
        NProto::TWriteBlocksLocalRequest,
        NProto::TWriteBlocksLocalResponse>,
    TDeferredRequestsHolder<
        NProto::TZeroBlocksRequest,
        NProto::TZeroBlocksResponse>>;

////////////////////////////////////////////////////////////////////////////////

class TSwitchableBlockStore final
    : public std::enable_shared_from_this<TSwitchableBlockStore>
    , public ISwitchableBlockStore
{
private:
    TLog Log;

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

    TAdaptiveLock DifferedRequestsLock;
    TDeferredRequestsHolders DeferredRequests;

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

        with_lock (DifferedRequestsLock) {
            WillSwitchToSecondary = false;

            if (SwitchedToSecondary) {
                STORAGE_INFO(
                    "Switching from "
                    << PrimaryClientInfo.DiskId.Quote() << " to "
                    << SecondaryClientInfo.DiskId.Quote() << " is completed");
            } else {
                STORAGE_INFO(
                    "Switching from " << PrimaryClientInfo.DiskId.Quote()
                                      << " is interrupted");
            }

            const TClientInfo& currentClientInfo =
                SwitchedToSecondary ? SecondaryClientInfo : PrimaryClientInfo;

            std::apply(
                [currentClientInfo](auto&... deferredRequests)
                {
                    (deferredRequests.ExecuteSavedRequests(currentClientInfo),
                     ...);
                },
                DeferredRequests);
        }
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

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                         \
    TFuture<NProto::T##name##Response> name(                           \
        TCallContextPtr callContext,                                   \
        std::shared_ptr<NProto::T##name##Request> request) override    \
    {                                                                  \
        constexpr bool isReadWriteRequest = IsReadWriteRequest(        \
            GetBlockStoreRequest<NProto::T##name##Request>());         \
        if constexpr (isReadWriteRequest) {                            \
            return ExecuteReadWriteRequest<NProto::T##name##Response>( \
                std::move(callContext),                                \
                std::move(request));                                   \
        }                                                              \
        return PrimaryClientInfo.Client->name(                         \
            std::move(callContext),                                    \
            std::move(request));                                       \
    }

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

private:
    template <typename TResponse, typename TRequest>
    TFuture<TResponse> ExecuteReadWriteRequest(
        TCallContextPtr callContext,
        std::shared_ptr<TRequest> request)
    {
        if (SwitchedToSecondary) {
            STORAGE_TRACE(
                "Forward " << typeid(TRequest).name() << " from "
                           << PrimaryClientInfo.DiskId.Quote() << " to "
                           << SecondaryClientInfo.DiskId.Quote());

            request->SetDiskId(SecondaryClientInfo.DiskId);
            request->SetSessionId(SecondaryClientInfo.SessionId);

            return TBlockStoreRequestAdapter::Execute(
                SecondaryClientInfo.Client.get(),
                std::move(callContext),
                std::move(request));
        }

        if (WillSwitchToSecondary) {
            with_lock (DifferedRequestsLock) {
                // A double check is necessary to avoid a race when a switch is
                // cancelled.
                if (WillSwitchToSecondary) {
                    STORAGE_TRACE(
                        "Save " << typeid(TRequest).name() << " from "
                                << PrimaryClientInfo.DiskId.Quote());

                    return std::get<
                               TDeferredRequestsHolder<TRequest, TResponse>>(
                               DeferredRequests)
                        .SaveRequest(
                            std::move(callContext),
                            std::move(request));
                }
            }
            return TBlockStoreRequestAdapter::Execute(
                SwitchedToSecondary ? SecondaryClientInfo.Client.get()
                                    : PrimaryClientInfo.Client.get(),
                std::move(callContext),
                std::move(request));
        }

        return TBlockStoreRequestAdapter::Execute(
            PrimaryClientInfo.Client.get(),
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
