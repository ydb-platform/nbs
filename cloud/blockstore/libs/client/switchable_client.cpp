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

template <typename T, typename U>
class TRequestHolder
{
private:
    struct TRequestInfo
    {
        TPromise<U> Promise;
        TCallContextPtr CallContext;
        std::shared_ptr<T> Request;
    };

    TAdaptiveLock RequestsLock;
    TVector<TRequestInfo> Requests;

public:
    TFuture<U> SaveRequest(
        TCallContextPtr callContext,
        std::shared_ptr<T> request)
    {
        with_lock (RequestsLock) {
            Requests.emplace_back(
                TRequestInfo{
                    .Promise = NewPromise<U>(),
                    .CallContext = std::move(callContext),
                    .Request = std::move(request)});
            return Requests.back().Promise;
        }
    }

    void Execute(IBlockStore* blockStore)
    {
        TVector<TRequestInfo> requests;
        with_lock (RequestsLock) {
            requests.swap(Requests);
        }

        for (auto& requestInfo: requests) {
            auto future = TBlockStoreAdapter::Execute(
                blockStore,
                std::move(requestInfo.CallContext),
                std::move(requestInfo.Request));
            future.Subscribe(
                [promise = std::move(requestInfo.Promise)](TFuture<U> f) mutable
                { promise.SetValue(f.ExtractValue()); });
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
    std::atomic_bool WillSwitchToSecondary{false};
    std::atomic_bool SwitchedToSecondary{false};

    TRequestHolder<NProto::TReadBlocksRequest, NProto::TReadBlocksResponse>
        ReadBlocksRequests;
    TRequestHolder<
        NProto::TReadBlocksLocalRequest,
        NProto::TReadBlocksLocalResponse>
        ReadBlocksLocalRequests;
    TRequestHolder<NProto::TWriteBlocksRequest, NProto::TWriteBlocksResponse>
        WriteBlocksRequests;
    TRequestHolder<
        NProto::TWriteBlocksLocalRequest,
        NProto::TWriteBlocksLocalResponse>
        WriteBlocksLocalRequests;
    TRequestHolder<NProto::TZeroBlocksRequest, NProto::TZeroBlocksResponse>
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
        STORAGE_INFO("Will switch form " << PrimaryClientInfo.DiskId.Quote());
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

        if (SwitchedToSecondary) {
            STORAGE_INFO(
                "Switching form " << PrimaryClientInfo.DiskId.Quote() << " to "
                                  << SecondaryClientInfo.DiskId.Quote()
                                  << " is completed");
        } else {
            STORAGE_INFO(
                "Switching form " << PrimaryClientInfo.DiskId.Quote()
                                  << " is interrupted");
        }

        IBlockStore* currentBlockStore = SwitchedToSecondary
                                             ? SecondaryClientInfo.Client.get()
                                             : PrimaryClientInfo.Client.get();
        ReadBlocksRequests.Execute(currentBlockStore);
        ReadBlocksLocalRequests.Execute(currentBlockStore);
        WriteBlocksRequests.Execute(currentBlockStore);
        WriteBlocksLocalRequests.Execute(currentBlockStore);
        ZeroBlocksRequests.Execute(currentBlockStore);

        WillSwitchToSecondary = false;
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
                STORAGE_WARN("Save " << #name);                                \
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

}   // namespace NCloud::NBlockStore
