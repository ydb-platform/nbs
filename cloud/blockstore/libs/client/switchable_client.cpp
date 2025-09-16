#include "switchable_client.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/vector.h>

#include <utility>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
concept HasSetDiskId = requires(T& obj) {
    {
        obj.SetDiskId(TString())
    } -> std::same_as<void>;
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
    {
        obj.SetSessionId(TString())
    } -> std::same_as<void>;
};

template <typename T>
void SetSessionIdIfExists(T& obj, const TString& sessionId)
{
    if constexpr (HasSetSessionId<T>) {
        obj.SetSessionId(sessionId);
    }
}

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
    std::atomic_bool SwitchedToSecondary{false};

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

    void Switch(
        IBlockStorePtr newClient,
        const TString& newDiskId,
        const TString& newSessionId) override
    {
        Y_ABORT_UNLESS(!SwitchedToSecondary);

        SecondaryClientInfo = {
            .Client = std::move(newClient),
            .DiskId = newDiskId,
            .SessionId = newSessionId};
        SwitchedToSecondary = true;

        STORAGE_INFO(
            "Switched from " << PrimaryClientInfo.DiskId.Quote() << " to "
                             << newDiskId.Quote());
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
        }                                                                      \
        return PrimaryClientInfo.Client->name(                                 \
            std::move(callContext),                                            \
            std::move(request));                                               \
    }

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD
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
