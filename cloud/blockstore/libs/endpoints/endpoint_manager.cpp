#include "endpoint_manager.h"

#include "endpoint_events.h"
#include "endpoint_listener.h"
#include "session_manager.h"

#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/hash.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NServer {

using namespace NThreading;

using namespace NCloud::NBlockStore::NClient;

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
    Y_DEBUG_ABORT_UNLESS(24 == GetFieldCount<NProto::TStartEndpointRequest>());
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
        && left.GetPersistent() == right.GetPersistent();
}

////////////////////////////////////////////////////////////////////////////////

class TEndpointManager final
    : public std::enable_shared_from_this<TEndpointManager>
    , public IEndpointManager
    , public IEndpointEventHandler
{
private:
    const IServerStatsPtr ServerStats;
    const TExecutorPtr Executor;
    const ISessionManagerPtr SessionManager;
    const THashMap<NProto::EClientIpcType, IEndpointListenerPtr> EndpointListeners;
    const TString NbdSocketSuffix;

    TLog Log;

    THashMap<TString, std::shared_ptr<NProto::TStartEndpointRequest>> Requests;

public:
    TEndpointManager(
            ILoggingServicePtr logging,
            IServerStatsPtr serverStats,
            TExecutorPtr executor,
            ISessionManagerPtr sessionManager,
            THashMap<NProto::EClientIpcType, IEndpointListenerPtr> listeners,
            TString nbdSocketSuffix)
        : ServerStats(std::move(serverStats))
        , Executor(std::move(executor))
        , SessionManager(std::move(sessionManager))
        , EndpointListeners(std::move(listeners))
        , NbdSocketSuffix(std::move(nbdSocketSuffix))
    {
        Log = logging->CreateLog("BLOCKSTORE_SERVER");
    }

    TFuture<NProto::TStartEndpointResponse> StartEndpoint(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TStartEndpointRequest> request) override;

    TFuture<NProto::TStopEndpointResponse> StopEndpoint(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TStopEndpointRequest> request) override;

    TFuture<NProto::TListEndpointsResponse> ListEndpoints(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TListEndpointsRequest> request) override;

    TFuture<NProto::TDescribeEndpointResponse> DescribeEndpoint(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TDescribeEndpointRequest> request) override;

    TFuture<NProto::TRefreshEndpointResponse> RefreshEndpoint(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TRefreshEndpointRequest> request) override;

    void OnVolumeConnectionEstablished(const TString& diskId) override;

private:
    NProto::TStartEndpointResponse StartEndpointImpl(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TStartEndpointRequest> request);

    NProto::TStopEndpointResponse StopEndpointImpl(
        TCallContextPtr ctx,
        const TString& socketPath,
        const NProto::THeaders& headers);

    NProto::TListEndpointsResponse ListEndpointsImpl(
        TCallContextPtr ctx,
        std::shared_ptr<NProto::TListEndpointsRequest> request);

    NProto::TRefreshEndpointResponse RefreshEndpointImpl(
        TCallContextPtr ctx,
        const TString& socketPath,
        const NProto::THeaders& headers);

    NProto::TStartEndpointResponse AlterEndpoint(
        TCallContextPtr ctx,
        const NProto::TStartEndpointRequest& newRequest,
        const NProto::TStartEndpointRequest& oldRequest);

    NProto::TError OpenEndpointSocket(
        const NProto::TStartEndpointRequest& request,
        const TSessionInfo& sessionInfo);

    TFuture<NProto::TError> CloseEndpointSocket(
        const NProto::TStartEndpointRequest& startRequest);

    std::shared_ptr<NProto::TStartEndpointRequest> CreateNbdStartEndpointRequest(
        const NProto::TStartEndpointRequest& request);

    void TrySwitchEndpoint(const TString& diskId);
};

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TStartEndpointResponse> TEndpointManager::StartEndpoint(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TStartEndpointRequest> request)
{
    return Executor->Execute([
        ctx = std::move(callContext),
        req = std::move(request),
        this] () mutable
    {
        return StartEndpointImpl(std::move(ctx), std::move(req));
    });
}

NProto::TStartEndpointResponse TEndpointManager::StartEndpointImpl(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TStartEndpointRequest> request)
{
    auto socketPath = request->GetUnixSocketPath();

    auto requestIt = Requests.find(socketPath);
    if (requestIt != Requests.end()) {
        const auto& startedEndpoint = *requestIt->second;
        return AlterEndpoint(std::move(ctx), *request, startedEndpoint);
    }

    if (request->GetClientId().empty()) {
        return TErrorResponse(
            E_ARGUMENT,
            TStringBuilder() << "ClientId shouldn't be empty");
    }

    auto createSessionFuture = SessionManager->CreateSession(ctx, *request);
    auto result = Executor->WaitFor(createSessionFuture);
    if (HasError(result)) {
        return TErrorResponse(result.GetError());
    }
    const auto& sessionInfo = result.GetResult();

    auto error = OpenEndpointSocket(*request, sessionInfo);
    if (HasError(error)) {
        auto future = SessionManager->RemoveSession(
            std::move(ctx),
            socketPath,
            request->GetHeaders());
        Executor->WaitFor(future);
        return TErrorResponse(error);
    }

    auto nbdRequest = CreateNbdStartEndpointRequest(*request);
    if (nbdRequest) {
        STORAGE_INFO("Start additional endpoint: " << *nbdRequest);
        auto error = OpenEndpointSocket(*nbdRequest, sessionInfo);

        if (HasError(error)) {
            auto closeFuture = CloseEndpointSocket(*request);
            Executor->WaitFor(closeFuture);
            auto removeFuture = SessionManager->RemoveSession(
                std::move(ctx),
                socketPath,
                request->GetHeaders());
            Executor->WaitFor(removeFuture);
            return TErrorResponse(error);
        }
    }

    if (auto c = ServerStats->GetEndpointCounter(request->GetIpcType())) {
        c->Inc();
    }
    auto [it, inserted] = Requests.emplace(socketPath, std::move(request));
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
        return TErrorResponse(
            S_ALREADY,
            TStringBuilder()
                << "endpoint " << socketPath.Quote()
                << " has already been started");
    }

    startedEndpoint.SetVolumeAccessMode(newRequest.GetVolumeAccessMode());
    startedEndpoint.SetVolumeMountMode(newRequest.GetVolumeMountMode());
    startedEndpoint.SetMountSeqNumber(newRequest.GetMountSeqNumber());

    if (!CompareRequests(newRequest, startedEndpoint)) {
        return TErrorResponse(
            E_INVALID_STATE,
            TStringBuilder()
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

TFuture<NProto::TStopEndpointResponse> TEndpointManager::StopEndpoint(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TStopEndpointRequest> request)
{
    return Executor->Execute([
        ctx = std::move(callContext),
        req = std::move(request),
        this] () mutable
    {
        return StopEndpointImpl(
            std::move(ctx),
            req->GetUnixSocketPath(),
            req->GetHeaders());
    });
}

NProto::TStopEndpointResponse TEndpointManager::StopEndpointImpl(
    TCallContextPtr ctx,
    const TString& socketPath,
    const NProto::THeaders& headers)
{
    auto it = Requests.find(socketPath);
    if (it == Requests.end()) {
        return TErrorResponse(
            S_FALSE,
            TStringBuilder()
                << "endpoint " << socketPath.Quote()
                << " hasn't been started yet");
    }

    auto startRequest = std::move(it->second);
    Requests.erase(it);
    if (auto c = ServerStats->GetEndpointCounter(startRequest->GetIpcType())) {
        c->Dec();
    }

    TVector<TFuture<NProto::TError>> closeSocketFutures;

    auto future = CloseEndpointSocket(*startRequest);
    closeSocketFutures.push_back(future);

    auto nbdRequest = CreateNbdStartEndpointRequest(*startRequest);
    if (nbdRequest) {
        STORAGE_INFO("Stop additional endpoint: "
            << nbdRequest->GetUnixSocketPath().Quote());
        auto future = CloseEndpointSocket(*nbdRequest);
        closeSocketFutures.push_back(future);
    }

    auto removeFuture = SessionManager->RemoveSession(
        std::move(ctx),
        socketPath,
        headers);
    Executor->WaitFor(removeFuture);

    NProto::TStopEndpointResponse response;

    for (const auto& closeSocketFuture: closeSocketFutures) {
        auto error = Executor->WaitFor(closeSocketFuture);

        if (HasError(error)) {
            response = TErrorResponse(error);
        }
    }

    return response;
}

TFuture<NProto::TListEndpointsResponse> TEndpointManager::ListEndpoints(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TListEndpointsRequest> request)
{
    return Executor->Execute([
        ctx = std::move(callContext),
        req = std::move(request),
        this] () mutable
    {
        return ListEndpointsImpl(std::move(ctx), std::move(req));
    });
}

NProto::TListEndpointsResponse TEndpointManager::ListEndpointsImpl(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TListEndpointsRequest> request)
{
    Y_UNUSED(ctx);
    Y_UNUSED(request);

    NProto::TListEndpointsResponse response;
    auto& endpoints = *response.MutableEndpoints();
    endpoints.Reserve(Requests.size());

    for (auto it: Requests) {
        auto& endpoint = *endpoints.Add();
        endpoint.CopyFrom(*it.second);
    }

    return response;
}

TFuture<NProto::TDescribeEndpointResponse> TEndpointManager::DescribeEndpoint(
    TCallContextPtr ctx,
    std::shared_ptr<NProto::TDescribeEndpointRequest> request)
{
    Y_UNUSED(ctx);

    NProto::TDescribeEndpointResponse response;

    auto profile = SessionManager->GetProfile(request->GetUnixSocketPath());
    if (HasError(profile)) {
        response.MutableError()->CopyFrom(profile.GetError());
    } else {
        response.MutablePerformanceProfile()->CopyFrom(profile.GetResult());
    }

    return MakeFuture(response);
}

TFuture<NProto::TRefreshEndpointResponse> TEndpointManager::RefreshEndpoint(
    TCallContextPtr callContext,
    std::shared_ptr<NProto::TRefreshEndpointRequest> request)
{
    return Executor->Execute([
        ctx = std::move(callContext),
        req = std::move(request),
        this] () mutable
    {
        return RefreshEndpointImpl(
            std::move(ctx),
            req->GetUnixSocketPath(),
            req->GetHeaders());
    });
}

NProto::TRefreshEndpointResponse TEndpointManager::RefreshEndpointImpl(
    TCallContextPtr ctx,
    const TString& socketPath,
    const NProto::THeaders& headers)
{
    auto it = Requests.find(socketPath);
    if (it == Requests.end()) {
        return TErrorResponse(
            S_FALSE,
            TStringBuilder()
                << "endpoint " << socketPath.Quote()
                << " not started");
    }

    auto ipcType = it->second->GetIpcType();
    auto listenerIt = EndpointListeners.find(ipcType);
    STORAGE_VERIFY(
        listenerIt != EndpointListeners.end(),
        TWellKnownEntityTypes::ENDPOINT,
        socketPath);
    const auto& listener = listenerIt->second;

    auto future = SessionManager->GetSession(ctx, socketPath, headers);
    auto result = Executor->WaitFor(future);
    if (HasError(result)) {
        return TErrorResponse(result.GetError());
    }

    const auto& sessionInfo = result.GetResult();
    auto error = listener->RefreshEndpoint(socketPath, sessionInfo.Volume);
    return TErrorResponse(error);
}

NProto::TError TEndpointManager::OpenEndpointSocket(
    const NProto::TStartEndpointRequest& request,
    const TSessionInfo& sessionInfo)
{
    auto ipcType = request.GetIpcType();
    auto listenerIt = EndpointListeners.find(ipcType);
    if (listenerIt == EndpointListeners.end()) {
        return TErrorResponse(
            E_ARGUMENT,
            TStringBuilder()
                << "unsupported endpoint type: " << static_cast<ui32>(ipcType));
    }
    auto listener = listenerIt->second;

    if (request.GetUnixSocketPath().size() > UnixSocketPathLengthLimit) {
        return TErrorResponse(
            E_ARGUMENT,
            TStringBuilder()
                << "Length of socket path should not be more than "
                << UnixSocketPathLengthLimit);
    }

    auto future = listener->StartEndpoint(
        request,
        sessionInfo.Volume,
        sessionInfo.Session);

    auto error = Executor->WaitFor(future);
    return error;
}

TFuture<NProto::TError> TEndpointManager::CloseEndpointSocket(
    const NProto::TStartEndpointRequest& startRequest)
{
    auto ipcType = startRequest.GetIpcType();
    auto listenerIt = EndpointListeners.find(ipcType);
    STORAGE_VERIFY(
        listenerIt != EndpointListeners.end(),
        TWellKnownEntityTypes::ENDPOINT,
        startRequest.GetUnixSocketPath());
    const auto& listener = listenerIt->second;

    return listener->StopEndpoint(startRequest.GetUnixSocketPath());
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

void TEndpointManager::TrySwitchEndpoint(const TString& diskId)
{
    auto it = FindIf(Requests, [&] (auto& v) {
        const auto& [_, req] = v;
        return req->GetDiskId() == diskId
            && req->GetIpcType() == NProto::IPC_VHOST;
    });

    if (it == Requests.end()) {
        return;
    }

    const auto& req = it->second;
    auto listenerIt = EndpointListeners.find(req->GetIpcType());
    STORAGE_VERIFY(
        listenerIt != EndpointListeners.end(),
        TWellKnownEntityTypes::ENDPOINT,
        req->GetUnixSocketPath());
    const auto& listener = listenerIt->second;

    auto ctx = MakeIntrusive<TCallContext>();
    auto future = SessionManager->GetSession(
        std::move(ctx),
        req->GetUnixSocketPath(),
        req->GetHeaders());
    auto result = Executor->WaitFor(future);
    if (HasError(result)) {
        return;
    }

    const auto& sessionInfo = result.GetResult();

    STORAGE_INFO("Switching endpoint for volume " << sessionInfo.Volume.GetDiskId()
        << ", IsFastPathEnabled=" << sessionInfo.Volume.GetIsFastPathEnabled()
        << ", Migrations=" << sessionInfo.Volume.GetMigrations().size());

    auto switchFuture = listener->SwitchEndpoint(
        *it->second,
        sessionInfo.Volume,
        sessionInfo.Session);
    auto error = Executor->WaitFor(switchFuture);
    if (HasError(error)) {
        ReportEndpointSwitchFailure(TStringBuilder()
            << "Failed to switch endpoint for volume "
            << sessionInfo.Volume.GetDiskId()
            << ", " << error.GetMessage());
    }
}

void TEndpointManager::OnVolumeConnectionEstablished(const TString& diskId)
{
    Y_UNUSED(diskId);

    // TODO: NBS-312 safely call TrySwitchEndpoint
    // Executor->ExecuteSimple([this, diskId] () {
    //     return TrySwitchEndpoint(diskId);
    // });
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

bool IsSameStartEndpointRequests(
    const NProto::TStartEndpointRequest& left,
    const NProto::TStartEndpointRequest& right)
{
    return CompareRequests(left, right);
}

}   // namespace NCloud::NBlockStore::NServer
