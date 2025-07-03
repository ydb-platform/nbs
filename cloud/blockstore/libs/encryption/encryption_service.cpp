#include "encryption_service.h"

#include "encryption_client.h"
#include "encryption_key.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/map.h>
#include <util/system/rwlock.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TEncryptionServiceWrapper
    : public IBlockStore
{
protected:
    const IBlockStorePtr Service;

public:
    TEncryptionServiceWrapper(IBlockStorePtr service)
        : Service(std::move(service))
    {}

    void Start() override
    {
        if (Service) {
            Service->Start();
        }
    }

    void Stop() override
    {
        if (Service) {
            Service->Stop();
        }
    }

    TStorageBuffer AllocateBuffer(size_t bytesCount) override
    {
        return Service->AllocateBuffer(bytesCount);
    }

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        auto session = GetSession(request->GetHeaders().GetClientId());        \
        if (session) {                                                         \
            return session->name(std::move(ctx), std::move(request));          \
        }                                                                      \
        return Service->name(std::move(ctx), std::move(request));              \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

private:
    virtual IBlockStorePtr GetSession(const TString& clientId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TMultipleEncryptionService
    : public TEncryptionServiceWrapper
    , public std::enable_shared_from_this<TMultipleEncryptionService>
{
private:
    const IEncryptionClientFactoryPtr EncryptionClientFactory;

    TLog Log;

    TRWMutex SessionLock;
    THashMap<TString, IBlockStorePtr> EncryptedSessions;

public:
    TMultipleEncryptionService(
            IBlockStorePtr service,
            ILoggingServicePtr logging,
            IEncryptionClientFactoryPtr encryptionClientFactory)
        : TEncryptionServiceWrapper(std::move(service))
        , EncryptionClientFactory(std::move(encryptionClientFactory))
    {
        Log = logging->CreateLog("BLOCKSTORE_SERVER");
    }

    TFuture<NProto::TCreateVolumeResponse> CreateVolume(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TCreateVolumeRequest> request) override
    {
        auto& encryptionSpec = *request->MutableEncryptionSpec();
        if (encryptionSpec.GetMode() != NProto::NO_ENCRYPTION &&
            encryptionSpec.HasKeyPath())
        {
            encryptionSpec.ClearKeyPath();
        }

        return Service->CreateVolume(std::move(ctx), std::move(request));
    }

    TFuture<NProto::TMountVolumeResponse> MountVolume(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TMountVolumeRequest> request) override
    {
        auto session = GetSession(request->GetHeaders().GetClientId());
        if (session) {
            return session->MountVolume(std::move(ctx), std::move(request));
        }

        const auto& encryptionSpec = request->GetEncryptionSpec();

        STORAGE_INFO(
            TRequestInfo(
                EBlockStoreRequest::MountVolume,
                ctx->RequestId,
                request->GetDiskId(),
                request->GetHeaders().GetClientId())
            << " start creating encryption client " << encryptionSpec);

        NThreading::TFuture<IEncryptionClientFactory::TResponse> future;

        if (request->GetDisableEncryption()) {
            future = MakeFuture(TResultOrError{Service});
        } else {
            future = EncryptionClientFactory->CreateEncryptionClient(
                Service,
                encryptionSpec,
                request->GetDiskId());
        }

        return future.Apply([
            weakPtr = weak_from_this(),
            ctx = std::move(ctx),
            request = std::move(request)] (const auto& f)
        {
            auto ptr = weakPtr.lock();
            if (!ptr) {
                return MakeFuture<NProto::TMountVolumeResponse>(TErrorResponse(
                    E_REJECTED,
                    "Multiple encryption service is destroyed"));
            }

            auto& Log = ptr->Log;

            const auto& sessionOrError = f.GetValue();

            ELogPriority logPriority = HasError(sessionOrError)
                                           ? ELogPriority::TLOG_ERR
                                           : ELogPriority::TLOG_INFO;
            STORAGE_LOG(
                logPriority,
                TRequestInfo(
                    EBlockStoreRequest::MountVolume,
                    ctx->RequestId,
                    request->GetDiskId(),
                    request->GetHeaders().GetClientId())
                << " finish creating encryption client: "
                << FormatError(sessionOrError.GetError()));

            if (HasError(sessionOrError)) {
                return MakeFuture<NProto::TMountVolumeResponse>(TErrorResponse(
                    sessionOrError.GetError()));
            }

            auto session = sessionOrError.GetResult();
            ptr->AddSession(*request, session);
            return session->MountVolume(std::move(ctx), std::move(request));
        });
    }

    TFuture<NProto::TUnmountVolumeResponse> UnmountVolume(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TUnmountVolumeRequest> request) override
    {
        auto session = GetSession(request->GetHeaders().GetClientId());
        if (!session) {
            return Service->UnmountVolume(std::move(ctx), std::move(request));
        }

        auto req = *request;
        auto future = session->UnmountVolume(std::move(ctx), std::move(request));

        return future.Apply([
            weakPtr = weak_from_this(),
            req = std::move(req)] (const auto& f)
        {
            const auto& response = f.GetValue();
            if (HasError(response)) {
                return response;
            }

            auto ptr = weakPtr.lock();
            if (!ptr) {
                return static_cast<NProto::TUnmountVolumeResponse>(
                    TErrorResponse(E_REJECTED,
                        "Multiple encryption service is destroyed"));
            }

            ptr->RemoveSession(req);
            return response;
        });
    }

private:
    IBlockStorePtr GetSession(const TString& clientId) override
    {
        TReadGuard readGuard(SessionLock);

        auto it = EncryptedSessions.find(clientId);
        if (it == EncryptedSessions.end()) {
            return nullptr;
        }

        return it->second;
    }

    void AddSession(
        const NProto::TMountVolumeRequest& request,
        IBlockStorePtr session)
    {
        const auto& clientId = request.GetHeaders().GetClientId();
        STORAGE_INFO("Add new encryption session"
            << " [d: " << request.GetDiskId() << "]"
            << " [c: " << clientId << "]");

        TWriteGuard writeGuard(SessionLock);
        EncryptedSessions.insert_or_assign(clientId, std::move(session));
    }

    void RemoveSession(
        const NProto::TUnmountVolumeRequest& request)
    {
        const auto& clientId = request.GetHeaders().GetClientId();
        STORAGE_INFO("Remove encryption session"
            << ", [d: " << request.GetDiskId() << "]"
            << ", [c: " << clientId << "]");

        TWriteGuard writeGuard(SessionLock);
        EncryptedSessions.erase(clientId);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockStorePtr CreateMultipleEncryptionService(
    IBlockStorePtr service,
    ILoggingServicePtr logging,
    IEncryptionClientFactoryPtr encryptionClientFactory)
{
    return std::make_shared<TMultipleEncryptionService>(
        std::move(service),
        std::move(logging),
        std::move(encryptionClientFactory));
}

}   // namespace NCloud::NBlockStore
