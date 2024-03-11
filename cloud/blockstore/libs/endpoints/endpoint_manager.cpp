#include "endpoint_manager.h"

#include "endpoint_events.h"
#include "endpoint_listener.h"
#include "session_manager.h"

#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/nbd/device.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/hash.h>
#include <util/generic/overloaded.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NServer {

using namespace NClient;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
int GetFieldCount()
{
    return T::GetDescriptor()->field_count();
}

bool CompareRequests(
    const NProto::TKmsKey& left,
    const NProto::TKmsKey& right)
{
    Y_DEBUG_ABORT_UNLESS(3 == GetFieldCount<NProto::TKmsKey>());
    return left.GetKekId() == right.GetKekId()
        && left.GetEncryptedDEK() == right.GetEncryptedDEK()
        && left.GetTaskId() == right.GetTaskId();
}

bool CompareRequests(
    const NProto::TKeyPath& left,
    const NProto::TKeyPath& right)
{
    Y_DEBUG_ABORT_UNLESS(3 == GetFieldCount<NProto::TKeyPath>());
    return left.GetKeyringId() == right.GetKeyringId()
        && left.GetFilePath() == right.GetFilePath()
        && CompareRequests(left.GetKmsKey(), right.GetKmsKey());
}

bool CompareRequests(
    const NProto::TEncryptionSpec& left,
    const NProto::TEncryptionSpec& right)
{
    Y_DEBUG_ABORT_UNLESS(3 == GetFieldCount<NProto::TEncryptionSpec>());
    return left.GetMode() == right.GetMode()
        && CompareRequests(left.GetKeyPath(), right.GetKeyPath())
        && left.GetKeyHash() == right.GetKeyHash();
}

bool CompareRequests(
    const NProto::TClientProfile& left,
    const NProto::TClientProfile& right)
{
    Y_DEBUG_ABORT_UNLESS(2 == GetFieldCount<NProto::TClientProfile>());
    return left.GetCpuUnitCount() == right.GetCpuUnitCount()
        && left.GetHostType() == right.GetHostType();
}

bool CompareRequests(
    const NProto::TClientMediaKindPerformanceProfile& left,
    const NProto::TClientMediaKindPerformanceProfile& right)
{
    Y_DEBUG_ABORT_UNLESS(4 == GetFieldCount<NProto::TClientMediaKindPerformanceProfile>());
    return left.GetMaxReadIops() == right.GetMaxReadIops()
        && left.GetMaxWriteIops() == right.GetMaxWriteIops()
        && left.GetMaxReadBandwidth() == right.GetMaxReadBandwidth()
        && left.GetMaxWriteBandwidth() == right.GetMaxWriteBandwidth();
}

bool CompareRequests(
    const NProto::TClientPerformanceProfile& left,
    const NProto::TClientPerformanceProfile& right)
{
    Y_DEBUG_ABORT_UNLESS(7 == GetFieldCount<NProto::TClientPerformanceProfile>());
    return CompareRequests(left.GetHDDProfile(), right.GetHDDProfile())
        && CompareRequests(left.GetSSDProfile(), right.GetSSDProfile())
        && CompareRequests(left.GetNonreplProfile(), right.GetNonreplProfile())
        && CompareRequests(left.GetMirror2Profile(), right.GetMirror2Profile())
        && CompareRequests(left.GetMirror3Profile(), right.GetMirror3Profile())
        && CompareRequests(left.GetHddNonreplProfile(), right.GetHddNonreplProfile())
        && left.GetBurstTime() == right.GetBurstTime();
}

bool CompareRequests(
    const NProto::TStartEndpointRequest& left,
    const NProto::TStartEndpointRequest& right)
{
    Y_DEBUG_ABORT_UNLESS(25 == GetFieldCount<NProto::TStartEndpointRequest>());
    return left.GetUnixSocketPath() == right.GetUnixSocketPath()
        && left.GetDiskId() == right.GetDiskId()
        && left.GetInstanceId() == right.GetInstanceId()
        && left.GetVolumeAccessMode() == right.GetVolumeAccessMode()
        && left.GetVolumeMountMode() == right.GetVolumeMountMode()
        && left.GetIpcType() == right.GetIpcType()
        && left.GetClientVersionInfo() == right.GetClientVersionInfo()
        && left.GetThrottlingDisabled() == right.GetThrottlingDisabled()
        && left.GetMountSeqNumber() == right.GetMountSeqNumber()
        && left.GetClientId() == right.GetClientId()
        && CompareRequests(left.GetClientProfile(), right.GetClientProfile())
        && CompareRequests(left.GetClientPerformanceProfile(), right.GetClientPerformanceProfile())
        && left.GetVhostQueuesCount() == right.GetVhostQueuesCount()
        && left.GetRequestTimeout() == right.GetRequestTimeout()
        && left.GetRetryTimeout() == right.GetRetryTimeout()
        && left.GetRetryTimeoutIncrement() == right.GetRetryTimeoutIncrement()
        && left.GetUnalignedRequestsDisabled() == right.GetUnalignedRequestsDisabled()
        && CompareRequests(left.GetEncryptionSpec(), right.GetEncryptionSpec())
        && left.GetSendNbdMinBlockSize() == right.GetSendNbdMinBlockSize()
        && left.GetMountFlags() == right.GetMountFlags()
        && left.GetDeviceName() == right.GetDeviceName()
        && std::equal(
            left.GetClientCGroups().begin(),
            left.GetClientCGroups().end(),
            right.GetClientCGroups().begin(),
            right.GetClientCGroups().end())
        && left.GetPersistent() == right.GetPersistent()
        && left.GetNbdDeviceFile() == right.GetNbdDeviceFile();
}

bool CompareRequests(
    const NProto::TStopEndpointRequest& left,
    const NProto::TStopEndpointRequest& right)
{
    Y_DEBUG_ABORT_UNLESS(2 == GetFieldCount<NProto::TStopEndpointRequest>());
    return left.GetUnixSocketPath() == right.GetUnixSocketPath();
}

bool CompareRequests(
    const NProto::TRefreshEndpointRequest& left,
    const NProto::TRefreshEndpointRequest& right)
{
    Y_DEBUG_ABORT_UNLESS(2 == GetFieldCount<NProto::TRefreshEndpointRequest>());
    return left.GetUnixSocketPath() == right.GetUnixSocketPath();
}

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    struct T##name##Method                                                     \
    {                                                                          \
        using TRequest = NProto::T##name##Request;                             \
        using TResponse = NProto::T##name##Response;                           \
    };
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_ENDPOINT_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

class TSwitchEndpointRequest
{
private:
    TString DiskId;
    TString UnixSocketPath;
    TString Reason;

public:
    const TString& GetDiskId() const
    {
        return DiskId;
    }

    void SetDiskId(const TString& diskId)
    {
        DiskId = diskId;
    }

    const TString& GetUnixSocketPath() const
    {
        return UnixSocketPath;
    }

    void SetUnixSocketPath(const TString& socketPath)
    {
        UnixSocketPath = socketPath;
    }

    const TString& GetReason() const
    {
        return Reason;
    }

    void SetReason(const TString& reason)
    {
        Reason = reason;
    }

};

////////////////////////////////////////////////////////////////////////////////

bool CompareRequests(
    const TSwitchEndpointRequest& left,
    const TSwitchEndpointRequest& right)
{
    return left.GetDiskId() == right.GetDiskId()
        && left.GetUnixSocketPath() == right.GetUnixSocketPath()
        && left.GetReason() == right.GetReason();
}

////////////////////////////////////////////////////////////////////////////////

struct TSwitchEndpointMethod
{
    using TRequest = TSwitchEndpointRequest;
    using TResponse = NProto::TError;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
struct TRequestState
{
    typename TMethod::TRequest Request;
    TFuture<typename TMethod::TResponse> Result;
};

////////////////////////////////////////////////////////////////////////////////

struct TEndpoint
{
    std::shared_ptr<NProto::TStartEndpointRequest> Request;
    NBD::IDeviceConnectionPtr Device;
};

////////////////////////////////////////////////////////////////////////////////

class TEndpointManager final
    : public IEndpointManager
    , public IEndpointEventHandler
{
private:
    const ILoggingServicePtr Logging;
    const IServerStatsPtr ServerStats;
    const TExecutorPtr Executor;
    const ISessionManagerPtr SessionManager;
    const THashMap<NProto::EClientIpcType, IEndpointListenerPtr> EndpointListeners;
    const TString NbdSocketSuffix;

    TLog Log;

    using TRequestStateVariant = std::variant<
        TRequestState<TStartEndpointMethod>,
        TRequestState<TStopEndpointMethod>,
        TRequestState<TRefreshEndpointMethod>,
        TRequestState<TSwitchEndpointMethod>
    >;
    THashMap<TString, TRequestStateVariant> ProcessingSockets;

    THashMap<TString, TEndpoint> Endpoints;

public:
    TEndpointManager(
            ILoggingServicePtr logging,
            IServerStatsPtr serverStats,
            TExecutorPtr executor,
            ISessionManagerPtr sessionManager,
            THashMap<NProto::EClientIpcType, IEndpointListenerPtr> listeners,
            TString nbdSocketSuffix)
        : Logging(std::move(logging))
        , ServerStats(std::move(serverStats))
        , Executor(std::move(executor))
        , SessionManager(std::move(sessionManager))
        , EndpointListeners(std::move(listeners))
        , NbdSocketSuffix(std::move(nbdSocketSuffix))
    {
        Log = Logging->CreateLog("BLOCKSTORE_SERVER");
    }

#define ENDPOINT_IMPLEMENT_METHOD(name, specifier, ...)                        \
    TFuture<T##name##Method::TResponse> name(                                  \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<T##name##Method::TRequest> request) specifier          \
    {                                                                          \
        return Executor->Execute([                                             \
            ctx = std::move(callContext),                                      \
            req = std::move(request),                                          \
            this] () mutable                                                   \
        {                                                                      \
            return Do##name(std::move(ctx), std::move(req));                   \
        });                                                                    \
    }                                                                          \
                                                                               \
    T##name##Method::TResponse Do##name(                                       \
        TCallContextPtr ctx,                                                   \
        std::shared_ptr<T##name##Method::TRequest> req);                       \
// ENDPOINT_IMPLEMENT_METHOD

    ENDPOINT_IMPLEMENT_METHOD(StartEndpoint, override)
    ENDPOINT_IMPLEMENT_METHOD(StopEndpoint, override)
    ENDPOINT_IMPLEMENT_METHOD(ListEndpoints, override)
    ENDPOINT_IMPLEMENT_METHOD(DescribeEndpoint, override)
    ENDPOINT_IMPLEMENT_METHOD(RefreshEndpoint, override)
    ENDPOINT_IMPLEMENT_METHOD(SwitchEndpoint, )

#undef ENDPOINT_IMPLEMENT_METHOD

    TFuture<NProto::TError> SwitchEndpointIfNeeded(
        const TString& diskId,
        const TString& reason) override;

private:
    NProto::TStartEndpointResponse StartEndpointImpl(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TStartEndpointRequest> request);

    NProto::TStopEndpointResponse StopEndpointImpl(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TStopEndpointRequest> request);

    NProto::TRefreshEndpointResponse RefreshEndpointImpl(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TRefreshEndpointRequest> request);

    NProto::TError SwitchEndpointImpl(
        TCallContextPtr ctx,
        std::shared_ptr<TSwitchEndpointRequest> request);

    NProto::TStartEndpointResponse AlterEndpoint(
        TCallContextPtr ctx,
        const NProto::TStartEndpointRequest& newRequest,
        const NProto::TStartEndpointRequest& oldRequest);

    NProto::TError OpenAllEndpointSockets(
        const NProto::TStartEndpointRequest& request,
        const TSessionInfo& sessionInfo);

    NProto::TError OpenEndpointSocket(
        const NProto::TStartEndpointRequest& request,
        const TSessionInfo& sessionInfo);

    void CloseAllEndpointSockets(const NProto::TStartEndpointRequest& request);
    void CloseEndpointSocket(const NProto::TStartEndpointRequest& request);

    std::shared_ptr<NProto::TStartEndpointRequest> CreateNbdStartEndpointRequest(
        const NProto::TStartEndpointRequest& request);

    TResultOrError<NBD::IDeviceConnectionPtr> StartNbdDevice(
        std::shared_ptr<NProto::TStartEndpointRequest> request);

    template <typename T>
    void RemoveSession(TCallContextPtr ctx, const T& request)
    {
        auto future = SessionManager->RemoveSession(
            std::move(ctx),
            request.GetUnixSocketPath(),
            request.GetHeaders());

        auto error = Executor->WaitFor(future);
        if (HasError(error)) {
            STORAGE_ERROR("Failed to remove session: " << FormatError(error));
        }
    }

    template <typename TMethod>
    TPromise<typename TMethod::TResponse> AddProcessingSocket(
        const typename TMethod::TRequest& request)
    {
        auto promise = NewPromise<typename TMethod::TResponse>();
        const auto& socketPath = request.GetUnixSocketPath();

        auto it = ProcessingSockets.find(socketPath);
        if (it != ProcessingSockets.end()) {
            const auto& st = it->second;
            auto* state = std::get_if<TRequestState<TMethod>>(&st);
            if (!state) {
                auto response = TErrorResponse(E_REJECTED, TStringBuilder()
                    << "endpoint " << socketPath.Quote()
                    << " is " << GetProcessName(st) << " now");
                promise.SetValue(response);
                return promise;
            }

            if (!CompareRequests(request, state->Request)) {
                auto response = TErrorResponse(E_REJECTED, TStringBuilder()
                    << "endpoint " << socketPath.Quote()
                    << " is " << GetProcessName(st) << " now with other args");
                promise.SetValue(response);
                return promise;
            }

            auto response = Executor->WaitFor(state->Result);
            promise.SetValue(response);
            return promise;
        }

        auto [_, inserted] = ProcessingSockets.emplace(
            socketPath,
            TRequestState<TMethod>{request, promise.GetFuture()});
        Y_ABORT_UNLESS(inserted);

        return promise;
    }

    void RemoveProcessingSocket(const TString& socketPath)
    {
        ProcessingSockets.erase(socketPath);
    }

    TString GetProcessName(const TRequestStateVariant& st)
    {
        return std::visit(TOverloaded{
            [] (const TRequestState<TStartEndpointMethod>&) {
                return "starting";
            },
            [] (const TRequestState<TStopEndpointMethod>&) {
                return "stopping";
            },
            [] (const TRequestState<TRefreshEndpointMethod>&) {
                return "refreshing";
            },
            [] (const TRequestState<TSwitchEndpointMethod>&) {
                return "switching";
            },
            [](const auto&) {
                return "busy (undefined process)";
            }
        }, st);
    }
};

////////////////////////////////////////////////////////////////////////////////

NProto::TStartEndpointResponse TEndpointManager::DoStartEndpoint(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TStartEndpointRequest> request)
{
    auto socketPath = request->GetUnixSocketPath();

    auto promise = AddProcessingSocket<TStartEndpointMethod>(*request);
    if (promise.HasValue()) {
        return promise.ExtractValue();
    }

    auto response = StartEndpointImpl(std::move(ctx), std::move(request));
    promise.SetValue(response);

    RemoveProcessingSocket(socketPath);
    return response;
}

NProto::TStartEndpointResponse TEndpointManager::StartEndpointImpl(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TStartEndpointRequest> request)
{
    const auto& socketPath = request->GetUnixSocketPath();

    auto it = Endpoints.find(socketPath);
    if (it != Endpoints.end()) {
        const auto& startedEndpoint = *it->second.Request;
        return AlterEndpoint(std::move(ctx), *request, startedEndpoint);
    }

    auto future = SessionManager->CreateSession(ctx, *request);
    auto [sessionInfo, error] = Executor->WaitFor(future);
    if (HasError(error)) {
        return TErrorResponse(error);
    }

    error = OpenAllEndpointSockets(*request, sessionInfo);
    if (HasError(error)) {
        RemoveSession(std::move(ctx), *request);
        return TErrorResponse(error);
    }

    auto deviceOrError = StartNbdDevice(request);
    if (HasError(deviceOrError)) {
        CloseAllEndpointSockets(*request);
        RemoveSession(std::move(ctx), *request);
        return TErrorResponse(deviceOrError.GetError());
    }

    TEndpoint endpoint = {
        .Request = request,
        .Device = deviceOrError.GetResult(),
    };

    if (auto c = ServerStats->GetEndpointCounter(request->GetIpcType())) {
        c->Inc();
    }
    auto [_, inserted] = Endpoints.emplace(socketPath, std::move(endpoint));
    STORAGE_VERIFY(inserted, TWellKnownEntityTypes::ENDPOINT, socketPath);

    NProto::TStartEndpointResponse response;
    response.MutableVolume()->CopyFrom(sessionInfo.Volume);
    return response;
}

NProto::TStartEndpointResponse TEndpointManager::AlterEndpoint(
    TCallContextPtr ctx,
    const NProto::TStartEndpointRequest& newRequest,
    const NProto::TStartEndpointRequest& oldRequest)
{
    const auto& socketPath = newRequest.GetUnixSocketPath();

    auto startedEndpoint = oldRequest;

    // NBS-3018
    if (!CompareRequests(
        oldRequest.GetClientProfile(),
        newRequest.GetClientProfile()))
    {
        STORAGE_WARN("Modified ClientProfile will be ignored for endpoint: "
            << socketPath.Quote());

        startedEndpoint.MutableClientProfile()->CopyFrom(
            newRequest.GetClientProfile());
    }

    // CLOUD-98154
    if (oldRequest.GetDeviceName() != newRequest.GetDeviceName()) {
        STORAGE_WARN("Modified DeviceName will be ignored for endpoint: "
            << socketPath.Quote());

        startedEndpoint.SetDeviceName(newRequest.GetDeviceName());
    }

    if (CompareRequests(newRequest, startedEndpoint)) {
        return TErrorResponse(S_ALREADY, TStringBuilder()
            << "endpoint " << socketPath.Quote()
            << " has already been started");
    }

    startedEndpoint.SetVolumeAccessMode(newRequest.GetVolumeAccessMode());
    startedEndpoint.SetVolumeMountMode(newRequest.GetVolumeMountMode());
    startedEndpoint.SetMountSeqNumber(newRequest.GetMountSeqNumber());

    if (!CompareRequests(newRequest, startedEndpoint)) {
        return TErrorResponse(E_INVALID_STATE, TStringBuilder()
            << "endpoint " << socketPath.Quote()
            << " has already been started with other args");
    }

    auto future = SessionManager->AlterSession(
        ctx,
        socketPath,
        newRequest.GetVolumeAccessMode(),
        newRequest.GetVolumeMountMode(),
        newRequest.GetMountSeqNumber(),
        newRequest.GetHeaders());

    if (auto error = Executor->WaitFor(future); HasError(error)) {
        return TErrorResponse(error);
    }

    auto [sessionInfo, error] = Executor->WaitFor(SessionManager->GetSession(
        ctx,
        socketPath,
        newRequest.GetHeaders()));

    if (HasError(error)) {
        return TErrorResponse(error);
    }

    auto listenerIt = EndpointListeners.find(startedEndpoint.GetIpcType());
    STORAGE_VERIFY(
        listenerIt != EndpointListeners.end(),
        TWellKnownEntityTypes::ENDPOINT,
        socketPath);

    auto& listener = listenerIt->second;

    auto alterFuture = listener->AlterEndpoint(
        startedEndpoint,
        sessionInfo.Volume,
        sessionInfo.Session);

    return TErrorResponse(Executor->WaitFor(alterFuture));
}

NProto::TStopEndpointResponse TEndpointManager::DoStopEndpoint(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TStopEndpointRequest> request)
{
    auto socketPath = request->GetUnixSocketPath();

    auto promise = AddProcessingSocket<TStopEndpointMethod>(*request);
    if (promise.HasValue()) {
        return promise.ExtractValue();
    }

    auto response = StopEndpointImpl(std::move(ctx), std::move(request));
    promise.SetValue(response);

    RemoveProcessingSocket(socketPath);
    return response;
}

NProto::TStopEndpointResponse TEndpointManager::StopEndpointImpl(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TStopEndpointRequest> request)
{
    const auto& socketPath = request->GetUnixSocketPath();

    auto it = Endpoints.find(socketPath);
    if (it == Endpoints.end()) {
        return TErrorResponse(S_FALSE, TStringBuilder()
            << "endpoint " << socketPath.Quote()
            << " hasn't been started yet");
    }

    auto endpoint = std::move(it->second);
    Endpoints.erase(it);
    if (auto c = ServerStats->GetEndpointCounter(endpoint.Request->GetIpcType())) {
        c->Dec();
    }

    endpoint.Device->Stop();
    CloseAllEndpointSockets(*endpoint.Request);
    RemoveSession(std::move(ctx), *request);
    return {};
}

NProto::TListEndpointsResponse TEndpointManager::DoListEndpoints(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TListEndpointsRequest> request)
{
    Y_UNUSED(ctx);
    Y_UNUSED(request);

    NProto::TListEndpointsResponse response;
    auto& responseEndpoints = *response.MutableEndpoints();
    responseEndpoints.Reserve(Endpoints.size());

    for (auto [_, endpoint]: Endpoints) {
        responseEndpoints.Add()->CopyFrom(*endpoint.Request);
    }

    return response;
}

NProto::TDescribeEndpointResponse TEndpointManager::DoDescribeEndpoint(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TDescribeEndpointRequest> req)
{
    Y_UNUSED(ctx);

    NProto::TDescribeEndpointResponse response;

    auto [profile, err] = SessionManager->GetProfile(req->GetUnixSocketPath());
    if (HasError(err)) {
        response.MutableError()->CopyFrom(err);
    } else {
        response.MutablePerformanceProfile()->CopyFrom(profile);
    }

    return response;
}

NProto::TRefreshEndpointResponse TEndpointManager::DoRefreshEndpoint(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TRefreshEndpointRequest> request)
{
    auto socketPath = request->GetUnixSocketPath();

    auto promise = AddProcessingSocket<TRefreshEndpointMethod>(*request);
    if (promise.HasValue()) {
        return promise.ExtractValue();
    }

    auto response = RefreshEndpointImpl(std::move(ctx), std::move(request));
    promise.SetValue(response);

    RemoveProcessingSocket(socketPath);
    return response;
}

NProto::TRefreshEndpointResponse TEndpointManager::RefreshEndpointImpl(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TRefreshEndpointRequest> request)
{
    const auto& socketPath = request->GetUnixSocketPath();
    const auto& headers = request->GetHeaders();

    auto it = Endpoints.find(socketPath);
    if (it == Endpoints.end()) {
        return TErrorResponse(S_FALSE, TStringBuilder()
            << "endpoint " << socketPath.Quote() << " not started");
    }

    auto ipcType = it->second.Request->GetIpcType();
    auto listenerIt = EndpointListeners.find(ipcType);
    STORAGE_VERIFY(
        listenerIt != EndpointListeners.end(),
        TWellKnownEntityTypes::ENDPOINT,
        socketPath);
    const auto& listener = listenerIt->second;

    auto future = SessionManager->GetSession(ctx, socketPath, headers);
    auto [sessionInfo, error] = Executor->WaitFor(future);
    if (HasError(error)) {
        return TErrorResponse(error);
    }

    error = listener->RefreshEndpoint(socketPath, sessionInfo.Volume);
    return TErrorResponse(error);
}

NProto::TError TEndpointManager::OpenAllEndpointSockets(
    const NProto::TStartEndpointRequest& request,
    const TSessionInfo& sessionInfo)
{
    auto error = OpenEndpointSocket(request, sessionInfo);
    if (HasError(error)) {
        return error;
    }

    auto nbdRequest = CreateNbdStartEndpointRequest(request);
    if (nbdRequest) {
        STORAGE_INFO("Start additional endpoint: " << *nbdRequest);
        auto error = OpenEndpointSocket(*nbdRequest, sessionInfo);

        if (HasError(error)) {
            CloseEndpointSocket(request);
            return error;
        }
    }
    return {};
}

NProto::TError TEndpointManager::OpenEndpointSocket(
    const NProto::TStartEndpointRequest& request,
    const TSessionInfo& sessionInfo)
{
    auto ipcType = request.GetIpcType();
    auto listenerIt = EndpointListeners.find(ipcType);
    if (listenerIt == EndpointListeners.end()) {
        return TErrorResponse(E_ARGUMENT, TStringBuilder()
            << "unsupported endpoint type: " << static_cast<ui32>(ipcType));
    }
    auto listener = listenerIt->second;

    if (request.GetUnixSocketPath().size() > UnixSocketPathLengthLimit) {
        return TErrorResponse(E_ARGUMENT, TStringBuilder()
            << "Length of socket path should not be more than "
            << UnixSocketPathLengthLimit);
    }

    auto future = listener->StartEndpoint(
        request,
        sessionInfo.Volume,
        sessionInfo.Session);

    return Executor->WaitFor(future);
}

void TEndpointManager::CloseAllEndpointSockets(
    const NProto::TStartEndpointRequest& request)
{
    CloseEndpointSocket(request);

    auto nbdRequest = CreateNbdStartEndpointRequest(request);
    if (nbdRequest) {
        STORAGE_INFO("Stop additional endpoint: "
            << nbdRequest->GetUnixSocketPath().Quote());
        CloseEndpointSocket(*nbdRequest);
    }
}

void TEndpointManager::CloseEndpointSocket(
    const NProto::TStartEndpointRequest& request)
{
    auto ipcType = request.GetIpcType();
    const auto& socketPath = request.GetUnixSocketPath();

    auto listenerIt = EndpointListeners.find(ipcType);
    STORAGE_VERIFY(
        listenerIt != EndpointListeners.end(),
        TWellKnownEntityTypes::ENDPOINT,
        socketPath);
    const auto& listener = listenerIt->second;

    auto future = listener->StopEndpoint(socketPath);
    auto error = Executor->WaitFor(future);
    if (HasError(error)) {
        STORAGE_ERROR("Failed to close socket " << socketPath.Quote()
            << ", error: " << FormatError(error));
    }
}

using TStartEndpointRequestPtr = std::shared_ptr<NProto::TStartEndpointRequest>;

TStartEndpointRequestPtr TEndpointManager::CreateNbdStartEndpointRequest(
    const NProto::TStartEndpointRequest& request)
{
    if (request.GetIpcType() != NProto::IPC_GRPC || NbdSocketSuffix.empty()) {
        return nullptr;
    }

    auto socketPath = request.GetUnixSocketPath() + NbdSocketSuffix;

    auto nbdRequest = std::make_shared<NProto::TStartEndpointRequest>(request);
    nbdRequest->SetIpcType(NProto::IPC_NBD);
    nbdRequest->SetUnixSocketPath(socketPath);
    nbdRequest->SetUnalignedRequestsDisabled(true);
    nbdRequest->SetSendNbdMinBlockSize(true);
    return nbdRequest;
}

NProto::TError TEndpointManager::DoSwitchEndpoint(
    TCallContextPtr ctx,
    std::shared_ptr<TSwitchEndpointRequest> request)
{
    const auto& diskId = request->GetDiskId();

    auto it = FindIf(Endpoints, [&] (auto& v) {
        const auto& [_, endpoint] = v;
        return endpoint.Request->GetDiskId() == diskId
            && endpoint.Request->GetIpcType() == NProto::IPC_VHOST;
    });

    if (it == Endpoints.end()) {
        return TErrorResponse(S_OK);
    }

    auto socketPath = it->first;
    request->SetUnixSocketPath(socketPath);

    auto promise = AddProcessingSocket<TSwitchEndpointMethod>(*request);
    if (promise.HasValue()) {
        return promise.ExtractValue();
    }

    auto response = SwitchEndpointImpl(std::move(ctx), std::move(request));
    promise.SetValue(response);

    RemoveProcessingSocket(socketPath);
    return response;
}

NProto::TError TEndpointManager::SwitchEndpointImpl(
    TCallContextPtr ctx,
    std::shared_ptr<TSwitchEndpointRequest> request)
{
    const auto& socketPath = request->GetUnixSocketPath();

    auto it = Endpoints.find(socketPath);
    if (it == Endpoints.end()) {
        return TErrorResponse(S_FALSE, TStringBuilder()
            << "endpoint " << socketPath.Quote() << " not started");
    }

    auto startRequest = it->second.Request;
    auto listenerIt = EndpointListeners.find(startRequest->GetIpcType());
    STORAGE_VERIFY(
        listenerIt != EndpointListeners.end(),
        TWellKnownEntityTypes::ENDPOINT,
        socketPath);
    const auto& listener = listenerIt->second;

    auto future = SessionManager->GetSession(
        ctx,
        startRequest->GetUnixSocketPath(),
        startRequest->GetHeaders());
    auto [sessionInfo, error] = Executor->WaitFor(future);
    if (HasError(error)) {
        return error;
    }

    STORAGE_INFO("Switching endpoint"
        << ", reason=" << request->GetReason()
        << ", volume=" << sessionInfo.Volume.GetDiskId()
        << ", IsFastPathEnabled=" << sessionInfo.Volume.GetIsFastPathEnabled()
        << ", Migrations=" << sessionInfo.Volume.GetMigrations().size());

    auto switchFuture = listener->SwitchEndpoint(
        *startRequest,
        sessionInfo.Volume,
        sessionInfo.Session);
    error = Executor->WaitFor(switchFuture);
    if (HasError(error)) {
        ReportEndpointSwitchFailure(TStringBuilder()
            << "Failed to switch endpoint for volume "
            << sessionInfo.Volume.GetDiskId()
            << ", " << error.GetMessage());
    }

    return TErrorResponse(error);
}

TResultOrError<NBD::IDeviceConnectionPtr> TEndpointManager::StartNbdDevice(
    std::shared_ptr<NProto::TStartEndpointRequest> request)
{
    if (request->GetIpcType() != NProto::IPC_NBD ||
        request->GetNbdDeviceFile().empty())
    {
        return NBD::CreateDeviceConnectionStub();
    }

    auto device = NBD::CreateDeviceConnection(
        Logging,
        TNetworkAddress(TUnixSocketPath(request->GetUnixSocketPath())),
        request->GetNbdDeviceFile(),
        TDuration::Days(1));

    try {
        device->Start();
    } catch (...) {
        return MakeError(E_FAIL, CurrentExceptionMessage());
    }

    return device;
}

TFuture<NProto::TError> TEndpointManager::SwitchEndpointIfNeeded(
    const TString& diskId,
    const TString& reason)
{
    auto ctx = MakeIntrusive<TCallContext>();
    auto request = std::make_shared<TSwitchEndpointRequest>();
    request->SetDiskId(diskId);
    request->SetReason(reason);

    return SwitchEndpoint(std::move(ctx), std::move(request));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IEndpointManagerPtr CreateEndpointManager(
    ILoggingServicePtr logging,
    IServerStatsPtr serverStats,
    TExecutorPtr executor,
    IEndpointEventProxyPtr eventProxy,
    ISessionManagerPtr sessionManager,
    THashMap<NProto::EClientIpcType, IEndpointListenerPtr> listeners,
    TString nbdSocketSuffix)
{
    auto manager = std::make_shared<TEndpointManager>(
        std::move(logging),
        std::move(serverStats),
        std::move(executor),
        std::move(sessionManager),
        std::move(listeners),
        std::move(nbdSocketSuffix));
    eventProxy->Register(manager);
    return manager;
}

bool AreSameStartEndpointRequests(
    const NProto::TStartEndpointRequest& left,
    const NProto::TStartEndpointRequest& right)
{
    return CompareRequests(left, right);
}

}   // namespace NCloud::NBlockStore::NServer
