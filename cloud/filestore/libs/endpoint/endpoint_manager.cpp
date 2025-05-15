#include "endpoint_manager.h"

#include "listener.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/endpoint.h>
#include <cloud/filestore/libs/service/request.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/endpoints/iface/endpoints.h>

#include <contrib/ydb/core/protos/flat_tx_scheme.pb.h>

#include <util/folder/path.h>
#include <util/generic/guid.h>
#include <util/system/hostname.h>
#include <util/generic/map.h>
#include <util/system/sysstat.h>

namespace NCloud::NFileStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool CompareRequests(
    const NProto::TEndpointConfig& left,
    const NProto::TEndpointConfig& right)
{
    return left.GetFileSystemId() == right.GetFileSystemId()
        && left.GetClientId() == right.GetClientId()
        && left.GetSocketPath() == right.GetSocketPath()
        && left.GetSessionRetryTimeout() == right.GetSessionRetryTimeout()
        && left.GetSessionPingTimeout() == right.GetSessionPingTimeout()
        && left.GetServiceEndpoint() == right.GetServiceEndpoint()
        && left.GetMountSeqNumber() == right.GetMountSeqNumber()
        && left.GetReadOnly() == right.GetReadOnly();
}

bool CompareRequests(
    const NProto::TStartEndpointRequest& left,
    const NProto::TStartEndpointRequest& right)
{
    return CompareRequests(left.GetEndpoint(), right.GetEndpoint());
}

bool SessionUpdateRequired(
    const NProto::TEndpointConfig& left,
    const NProto::TEndpointConfig& right)
{
    return left.GetMountSeqNumber() != right.GetMountSeqNumber()
        || left.GetReadOnly() != right.GetReadOnly();
}

////////////////////////////////////////////////////////////////////////////////

struct TEndpointInfo
{
    IEndpointPtr Endpoint;
    NProto::TEndpointConfig Config;
};

////////////////////////////////////////////////////////////////////////////////

struct TStartingEndpointState
{
    NProto::TStartEndpointRequest Request;
    TFuture<NProto::TStartEndpointResponse> Result;

    TStartingEndpointState(
            const NProto::TStartEndpointRequest& request,
            const TFuture<NProto::TStartEndpointResponse>& result)
        : Request(request)
        , Result(result)
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TStoppingEndpointState
{
    TFuture<NProto::TStopEndpointResponse> Result;

    TStoppingEndpointState(const TFuture<NProto::TStopEndpointResponse>& result)
        : Result(result)
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TEndpointManager final
    : public IEndpointManager
    , public std::enable_shared_from_this<TEndpointManager>
{
private:
    const ILoggingServicePtr Logging;
    const IEndpointStoragePtr Storage;
    const IEndpointListenerPtr Listener;
    const ui32 SocketAccessMode;

    TExecutorPtr Executor = TExecutor::Create("SVC");
    TLog Log;

    THashMap<TString, TStartingEndpointState> StartingSockets;
    THashMap<TString, TStoppingEndpointState> StoppingSockets;

    TMutex EndpointsLock;
    bool DrainingStarted = false;
    TMap<TString, TEndpointInfo> Endpoints;

public:
    TEndpointManager(
            ILoggingServicePtr logging,
            IEndpointStoragePtr storage,
            IEndpointListenerPtr listener,
            ui32 socketAccessMode)
        : Logging(std::move(logging))
        , Storage(std::move(storage))
        , Listener(std::move(listener))
        , SocketAccessMode(socketAccessMode)
    {
        Log = Logging->CreateLog("NFS_SERVICE");
    }

    void Start() override
    {
        Executor->Start();
    }

    void Stop() override
    {
        Executor->Stop();
    }

    void Drain() override
    {
        auto g = Guard(EndpointsLock);
        DrainingStarted = true;

        TVector<TFuture<void>> futures;
        for (auto&& [_, endpoint]: Endpoints) {
            futures.push_back(endpoint.Endpoint->SuspendAsync());
        }
        WaitAll(futures).GetValueSync();
    }

#define FILESTORE_IMPLEMENT_METHOD(name, ...)                                  \
public:                                                                        \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(callContext);                                                 \
        return Executor->Execute([this, request = std::move(request)] {        \
            return Do##name(*request);                                         \
        });                                                                    \
    }                                                                          \
private:                                                                       \
    NProto::T##name##Response Do##name(                                        \
        const NProto::T##name##Request& request);                              \
// FILESTORE_IMPLEMENT_METHOD

    FILESTORE_ENDPOINT_SERVICE(FILESTORE_IMPLEMENT_METHOD)

#undef FILESTORE_IMPLEMENT_METHOD

private:
    TFuture<void> RestoreEndpoints() override
    {
        return Executor->Execute([weakSelf = weak_from_this()] () mutable {
            if (auto self = weakSelf.lock()) {
                auto future = self->DoRestoreEndpoints();
                self->Executor->WaitFor(future);
            }
        });
    }

    TFuture<void> DoRestoreEndpoints()
    {
        auto idsOrError = Storage->GetEndpointIds();

        if (HasError(idsOrError)) {
            STORAGE_ERROR("Failed to get endpoints from storage: "
                << FormatError(idsOrError.GetError()));
            return MakeFuture();
        }

        const auto& storedIds = idsOrError.GetResult();
        STORAGE_INFO("Found " << storedIds.size() << " endpoints in storage");

        auto clientId = CreateGuidAsString();
        auto originFqdn = GetFQDNHostName();

        TVector<TFuture<void>> futures;
        for (auto endpointId: storedIds) {
            STORAGE_INFO("Restoring endpoint, ID: " << endpointId.Quote());
            auto requestOrError = Storage->GetEndpoint(endpointId);
            if (HasError(requestOrError)) {
                STORAGE_WARN("Failed to restore endpoint. ID: " << endpointId
                    << ", error: " << FormatError(requestOrError.GetError()));
                continue;
            }

            auto request = DeserializeEndpoint<NProto::TStartEndpointRequest>(
                requestOrError.GetResult());

            if (!request) {
                // TODO: report critical error
                STORAGE_ERROR("Failed to deserialize request. ID: " << endpointId);
                continue;
            }

            auto requestId = CreateRequestId();
            request->MutableHeaders()->SetRequestId(requestId);
            request->MutableHeaders()->SetClientId(clientId);
            request->MutableHeaders()->SetOriginFqdn(originFqdn);

            auto socketPath = TFsPath(request->GetEndpoint().GetSocketPath());
            socketPath.Parent().MkDirs();

            auto future = StartEndpoint(
                MakeIntrusive<TCallContext>(requestId),
                std::move(request));

            future.Subscribe(
                [weakPtr = weak_from_this(), endpointId](const auto& f) {
                    if (auto ptr = weakPtr.lock()) {
                        const auto& response = f.GetValue();
                        ptr->HandleRestoredEndpoint(
                            endpointId,
                            response.GetError());
                    }
                });
            futures.push_back(future.IgnoreResult());
        }

        return WaitAll(futures);
    }

    void HandleRestoredEndpoint(
        const TString& endpointId,
        const NProto::TError& error)
    {
        if (HasError(error)) {
            STORAGE_ERROR("Failed to start endpoint: "
                << FormatError(error));
            if (error.GetCode() ==
                MAKE_SCHEMESHARD_ERROR(NKikimrScheme::StatusPathDoesNotExist))
            {
                STORAGE_INFO(
                    "Remove endpoint for non-existing filesystem. endpoint id: "
                    << endpointId.Quote());
                Storage->RemoveEndpoint(endpointId);
            }
        }
        else {
            STORAGE_INFO("Endpoint was restored. ID: " << endpointId.Quote());
        }
    }

    void AddStartingSocket(
        const TString& socketPath,
        const NProto::TStartEndpointRequest& request,
        const TFuture<NProto::TStartEndpointResponse>& result)
    {
        auto [it, inserted] = StartingSockets.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(socketPath),
            std::forward_as_tuple(request, result));
        Y_ABORT_UNLESS(inserted);
    }

    void RemoveStartingSocket(const TString& socketPath)
    {
        StartingSockets.erase(socketPath);
    }

    void AddStoppingSocket(
        const TString& socketPath,
        const TFuture<NProto::TStopEndpointResponse>& result)
    {
        auto [it, inserted] = StoppingSockets.emplace(socketPath, result);
        Y_ABORT_UNLESS(inserted);
    }

    void RemoveStoppingSocket(const TString& socketPath)
    {
        StoppingSockets.erase(socketPath);
    }

    NProto::TError StoreEndpointIfNeeded(const NProto::TEndpointConfig& config)
    {
        if (!config.GetPersistent()) {
            return {};
        }

        NProto::TStartEndpointRequest request;
        *request.MutableEndpoint() = config;

        auto [data, error] = SerializeEndpoint(request);
        if (HasError(error)) {
            return error;
        }

        error = Storage->AddEndpoint(config.GetSocketPath(), data);
        if (HasError(error)) {
            return error;
        }

        return {};
    }
};

////////////////////////////////////////////////////////////////////////////////

NProto::TStartEndpointResponse TEndpointManager::DoStartEndpoint(
    const NProto::TStartEndpointRequest& request)
{
    STORAGE_TRACE("StartEndpoint " << DumpMessage(request));

    auto g = Guard(EndpointsLock);
    if (DrainingStarted) {
        return TErrorResponse(E_REJECTED, "draining");
    }

    const auto& config = request.GetEndpoint();
    const auto& socketPath = config.GetSocketPath();

    if (StoppingSockets.contains(socketPath)) {
        return TErrorResponse(E_REJECTED, TStringBuilder()
            << "endpoint " << socketPath.Quote()
            << " is stopping now");
    }

    if (const auto* p = StartingSockets.FindPtr(socketPath)) {
        if (!CompareRequests(request, p->Request)) {
            return TErrorResponse(E_REJECTED, TStringBuilder()
                << "endpoint " << socketPath.Quote()
                << " is starting now with other args");
        }

        auto future = p->Result;
        Executor->WaitFor(future);
        return future.GetValue();
    }

    if (auto* endpoint = Endpoints.FindPtr(socketPath)) {
        const auto& newConfig = request.GetEndpoint();
        const auto& oldConfig = endpoint->Config;
        if (CompareRequests(newConfig, oldConfig)) {
            return TErrorResponse(S_ALREADY, TStringBuilder()
                << "endpoint " << socketPath.Quote()
                << " already started");
        } else if (SessionUpdateRequired(newConfig, oldConfig)) {
            const auto& config = request.GetEndpoint();
            endpoint->Config.SetReadOnly(config.GetReadOnly());
            endpoint->Config.SetMountSeqNumber(config.GetMountSeqNumber());

            auto readOnly = config.GetReadOnly();
            auto mountSeqNumber = config.GetMountSeqNumber();

            auto future = endpoint->Endpoint->AlterAsync(
                readOnly,
                mountSeqNumber).Apply(
                [=, this] (const TFuture<NProto::TError>& future) {
                    NProto::TStartEndpointResponse response;
                    auto error = future.GetValue();
                    if (!HasError(error)) {
                        endpoint->Config.SetReadOnly(readOnly);
                        endpoint->Config.SetMountSeqNumber(mountSeqNumber);
                        StoreEndpointIfNeeded(endpoint->Config);
                    }
                    response.MutableError()->CopyFrom(future.GetValue());
                    return response;
                });
            Executor->WaitFor(future);
            return future.GetValue();
        } else {
            return TErrorResponse(E_ARGUMENT, TStringBuilder()
                << "endpoint " << socketPath.Quote()
                << ": attempt to change non-modifiable parameters");
        }
    }

    auto endpoint = Listener->CreateEndpoint(config);
    auto future = endpoint->StartAsync().Apply(
        [socketPath, socketAccessMode = SocketAccessMode]
        (const TFuture<NProto::TError>& future) {
            NProto::TStartEndpointResponse response;
            response.MutableError()->CopyFrom(future.GetValue());
            if (HasError(response)) {
                return response;
            }

            Chmod(socketPath.c_str(), socketAccessMode);
            return response;
        });

    AddStartingSocket(socketPath, request, future);
    Executor->WaitFor(future);
    RemoveStartingSocket(socketPath);

    const auto& response = future.GetValue();
    if (SUCCEEDED(response.GetError().GetCode())) {
        StoreEndpointIfNeeded(config);
        Endpoints.emplace(socketPath, TEndpointInfo { endpoint, config });
    }

    return response;
}

NProto::TStopEndpointResponse TEndpointManager::DoStopEndpoint(
    const NProto::TStopEndpointRequest& request)
{
    STORAGE_TRACE("StopEndpoint " << DumpMessage(request));

    auto g = Guard(EndpointsLock);
    if (DrainingStarted) {
        return TErrorResponse(E_REJECTED, "draining");
    }

    const auto& socketPath = request.GetSocketPath();

    if (StartingSockets.contains(socketPath)) {
        return TErrorResponse(E_REJECTED, TStringBuilder()
            << "endpoint " << socketPath.Quote()
            << " is starting now");
    }

    if (const auto* p = StoppingSockets.FindPtr(socketPath)) {
        auto future = p->Result;
        Executor->WaitFor(future);
        return future.GetValue();
    }

    auto it = Endpoints.find(request.GetSocketPath());
    if (it == Endpoints.end()) {
        return TErrorResponse(S_FALSE, TStringBuilder()
            << "endpoint " << socketPath.Quote()
            << " not found");
    }

    auto endpoint = it->second.Endpoint;
    auto future = endpoint->StopAsync().Apply(
        [] (const TFuture<void>&) {
            return NProto::TStopEndpointResponse{};
        });

    AddStoppingSocket(socketPath, future);
    Executor->WaitFor(future);
    RemoveStoppingSocket(socketPath);

    const auto& response = future.GetValue();
    if (SUCCEEDED(response.GetError().GetCode())) {
        if (auto error = Storage->RemoveEndpoint(request.GetSocketPath());
            HasError(error)
                && !HasProtoFlag(error.GetFlags(), NProto::EF_SILENT))
        {
            STORAGE_ERROR("Failed to remove endpoint from storage: "
                << FormatError(error));
        }
    }

    Endpoints.erase(it);
    return future.GetValue();
}

NProto::TListEndpointsResponse TEndpointManager::DoListEndpoints(
    const NProto::TListEndpointsRequest& request)
{
    STORAGE_TRACE("ListEndpoints " << DumpMessage(request));

    auto g = Guard(EndpointsLock);
    if (DrainingStarted) {
        return TErrorResponse(E_REJECTED, "draining");
    }

    NProto::TListEndpointsResponse response;
    for (const auto& [k, v]: Endpoints) {
        *response.AddEndpoints() = v.Config;
    }

    return response;
}

NProto::TKickEndpointResponse TEndpointManager::DoKickEndpoint(
    const NProto::TKickEndpointRequest& request)
{
    STORAGE_TRACE("KickEndpoint " << DumpMessage(request));

    auto requestOrError = Storage->GetEndpoint(
        ToString(request.GetKeyringId()));

    if (HasError(requestOrError)) {
        return TErrorResponse(requestOrError.GetError());
    }

    auto startRequest = DeserializeEndpoint<NProto::TStartEndpointRequest>(
        requestOrError.GetResult());
    auto startResponse = DoStartEndpoint(*startRequest);

    NProto::TKickEndpointResponse response;
    response.MutableError()->CopyFrom(startResponse.GetError());

    return response;
}

NProto::TPingResponse TEndpointManager::DoPing(
    const NProto::TPingRequest& request)
{
    STORAGE_TRACE("Ping " << DumpMessage(request));

    return {};
}

////////////////////////////////////////////////////////////////////////////////

class TNullEndpointManager final
    : public IEndpointManager
{
    void Start() override
    {}

    void Stop() override
    {}

    void Drain() override
    {}

    NThreading::TFuture<void> RestoreEndpoints() override
    {
        return MakeFuture();
    }

#define FILESTORE_IMPLEMENT_METHOD(name, ...)                                  \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        Y_UNUSED(callContext);                                                 \
        Y_UNUSED(request);                                                     \
        return MakeFuture<NProto::T##name##Response>();                        \
    }                                                                          \
// FILESTORE_IMPLEMENT_METHOD

    FILESTORE_ENDPOINT_SERVICE(FILESTORE_IMPLEMENT_METHOD)

#undef FILESTORE_IMPLEMENT_METHOD
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IEndpointManagerPtr CreateEndpointManager(
    ILoggingServicePtr logging,
    IEndpointStoragePtr storage,
    IEndpointListenerPtr listener,
    ui32 socketAccessMode)
{
    return std::make_shared<TEndpointManager>(
        std::move(logging),
        std::move(storage),
        std::move(listener),
        socketAccessMode);
}

IEndpointManagerPtr CreateNullEndpointManager()
{
    return std::make_shared<TNullEndpointManager>();
}

}   // namespace NCloud::NFileStore
