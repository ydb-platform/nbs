#include "volume_session_actor.h"

#include "service_actor.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/mount_token.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/common/media.h>

#include <contrib/ydb/core/tablet/tablet_setup.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/flags.h>
#include <util/generic/scope.h>
#include <util/string/builder.h>
#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NCloud::NStorage;

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 InitialAddClientMultiplier = 5;

constexpr TDuration RemountDelayWarn = TDuration::MilliSeconds(300);

////////////////////////////////////////////////////////////////////////////////

using TEvInternalMountVolumeResponsePtr =
    std::unique_ptr<TEvServicePrivate::TEvInternalMountVolumeResponse>;

////////////////////////////////////////////////////////////////////////////////

NProto::TError MakeErrorSilent(const NProto::TError& error)
{
    auto result = error;
    SetErrorProtoFlag(result, NProto::EF_SILENT);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

inline TString AccessModeToString(const NProto::EVolumeAccessMode mode)
{
    switch (mode) {
        case NProto::VOLUME_ACCESS_READ_WRITE:
            return "read-write";
        case NProto::VOLUME_ACCESS_READ_ONLY:
            return "read-only";
        case NProto::VOLUME_ACCESS_REPAIR:
            return "repair";
        case NProto::VOLUME_ACCESS_USER_READ_ONLY:
            return "user-read-only";
        default:
            Y_DEBUG_ABORT_UNLESS(false, "Unknown EVolumeAccessMode: %d", mode);
            return "undefined";
    }
}

void SendUpdateManuallyPreemptedVolumes(
    const TActorContext& ctx,
    const TString& diskId,
    NProto::EPreemptionSource newSource)
{
    using TRequest = TEvServicePrivate::TEvUpdateManuallyPreemptedVolume;
    auto request = std::make_unique<TRequest>(diskId, newSource);
    NCloud::Send(ctx, MakeStorageServiceId(), std::move(request));
}

template <typename TVolume>
bool NeedToSetEncryptionKeyHash(const TVolume& volume, const TString& keyHash)
{
    const auto& volumeEncryption = volume.GetEncryptionDesc();
    return
        volumeEncryption.GetMode() != NProto::NO_ENCRYPTION &&
        !volumeEncryption.GetKeyHash() &&
        keyHash;
}

////////////////////////////////////////////////////////////////////////////////

TEvInternalMountVolumeResponsePtr CreateInternalMountResponse(
    const NProto::TError& error,
    const TVolumeInfo& volumeInfo,
    const TStorageConfig& Config)
{
    const auto& result =
        error.GetCode() == E_NOT_FOUND ? MakeErrorSilent(error) : error;

    auto response =
        std::make_unique<TEvServicePrivate::TEvInternalMountVolumeResponse>(
            result);

    if (!HasError(error)) {
        Y_ABORT_UNLESS(volumeInfo.VolumeInfo.Defined());

        response->Record.SetSessionId(volumeInfo.SessionId);
        response->Record.MutableVolume()->CopyFrom(*volumeInfo.VolumeInfo);
        response->Record.SetInactiveClientsTimeout(
            static_cast<ui32>(Config.GetClientRemountPeriod().MilliSeconds()));
        response->Record.SetServiceVersionInfo(Config.GetServiceVersionInfo());
    }

    return response;
}

template <typename TSource>
void SendInternalMountVolumeResponse(
    const NActors::TActorContext& ctx,
    const TSource& source,
    const NProto::TError& error,
    const TVolumeInfo& volumeInfo,
    const TStorageConfig& config)
{
    NCloud::Reply(
        ctx,
        *source,
        CreateInternalMountResponse(error, volumeInfo, config));
}

////////////////////////////////////////////////////////////////////////////////

struct TMountRequestParams
{
    ui64 MountStartTick = 0;
    TDuration InitialAddClientTimeout;

    TActorId SessionActorId;
    TActorId VolumeClient;
    TActorId StartVolumeActor;

    NProto::EVolumeBinding BindingType = NProto::BINDING_REMOTE;
    NProto::EPreemptionSource PreemptionSource = NProto::SOURCE_NONE;

    bool IsLocalMounter = false;
    bool RejectOnAddClientTimeout = false;

    ui64 KnownTabletId = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TMountRequestActor final
    : public TActorBootstrapped<TMountRequestActor>
{
private:
    const TStorageConfigPtr Config;
    const TRequestInfoPtr RequestInfo;
    NProto::TMountVolumeRequest Request;
    const TString SessionId;
    bool AddClientRequestCompleted = false;

    // Most recent error code from suboperation (add client, start volume)
    NProto::TError Error;

    ui64 VolumeTabletId = 0;
    NProto::TVolume Volume;

    bool VolumeStarted = false;
    TMountRequestParams Params;
    NProto::EVolumeMountMode MountMode;
    const bool MountOptionsChanged;

    bool IsTabletAcquired = false;
    bool VolumeSessionRestartRequired = false;
    bool IsVolumeRestarting = false;

public:
    TMountRequestActor(
        TStorageConfigPtr config,
        TRequestInfoPtr requestInfo,
        NProto::TMountVolumeRequest request,
        TString sessionId,
        const TMountRequestParams& params,
        bool mountOptionsChanged);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeVolume(const TActorContext& ctx);

    void AddClient(const TActorContext& ctx, TDuration timeout);
    void WaitForVolume(const TActorContext& ctx, TDuration timeout);

    void RequestVolumeStart(const TActorContext& ctx);

    void RequestVolumeStop(const TActorContext& ctx);

    void NotifyAndDie(const TActorContext& ctx);

    void LockVolume(const TActorContext& ctx);
    void UnlockVolume(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleDescribeVolumeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDescribeVolumeError(
        const TActorContext& ctx,
        const NProto::TError& error);

    void HandleAlterVolumeResponse(
        const TEvService::TEvAlterVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAlterVolumeError(
        const TActorContext& ctx,
        const NProto::TError& error);

    void HandleVolumeAddClientResponse(
        const TEvVolume::TEvAddClientResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleWaitReadyResponse(
        const TEvVolume::TEvWaitReadyResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleStartVolumeResponse(
        const TEvServicePrivate::TEvStartVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleStopVolumeResponse(
        const TEvServicePrivate::TEvStopVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleLockTabletResponse(
        const TEvHiveProxy::TEvLockTabletResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleUnlockTabletResponse(
        const TEvHiveProxy::TEvUnlockTabletResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleTabletLockLost(
        const TEvHiveProxy::TEvTabletLockLost::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TMountRequestActor::TMountRequestActor(
        TStorageConfigPtr config,
        TRequestInfoPtr requestInfo,
        NProto::TMountVolumeRequest request,
        TString sessionId,
        const TMountRequestParams& params,
        bool mountOptionsChanged)
    : Config(std::move(config))
    , RequestInfo(std::move(requestInfo))
    , Request(std::move(request))
    , SessionId(std::move(sessionId))
    , Params(params)
    , MountOptionsChanged(mountOptionsChanged)
{
    MountMode = Request.GetVolumeMountMode();
    if (Params.BindingType == NProto::BINDING_REMOTE) {
        MountMode = NProto::VOLUME_MOUNT_REMOTE;
    // XXX the following 'else if' seems correct but breaks test
    //} else if (Params.BindingType == TVolumeInfo::LOCAL) {
    //    MountMode = NProto::VOLUME_MOUNT_LOCAL;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TMountRequestActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    LOG_INFO_S(ctx, TBlockStoreComponents::SERVICE,
        "Start mounting volume " << Request.GetDiskId().Quote() << " with"
        " mount mode: " << EVolumeMountMode_Name(Request.GetVolumeMountMode()) <<
        " binding: " << EVolumeBinding_Name(Params.BindingType) <<
        " source: " << EPreemptionSource_Name(Params.PreemptionSource));

    DescribeVolume(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TMountRequestActor::DescribeVolume(const TActorContext& ctx)
{
    const auto& diskId = Request.GetDiskId();

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Sending describe request for volume: %s",
        diskId.Quote().data());

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(diskId));
}

////////////////////////////////////////////////////////////////////////////////

void TMountRequestActor::HandleDescribeVolumeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        HandleDescribeVolumeError(ctx, error);
        return;
    }

    const auto& pathDescription = msg->PathDescription;
    // Verify assigned owner is correct
    const auto& volumeDescription =
        pathDescription.GetBlockStoreVolumeDescription();

    const auto& requestSource =
        Request.GetHeaders().GetInternal().GetRequestSource();
    if (IsDataChannel(requestSource)) {
        // Requests coming from data channel are authorized with mount token.
        TMountToken publicToken;
        auto parseStatus = publicToken.ParseString(
            volumeDescription.GetMountToken());

        if (parseStatus != TMountToken::EStatus::OK) {
            LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
                "Invalid mount token: %s",
                volumeDescription.GetMountToken().Quote().data());

            auto error = MakeError(E_FAIL, TStringBuilder()
                << "Cannot parse mount token (" << parseStatus << ")");
            HandleDescribeVolumeError(ctx, error);
            return;
        }

        const auto& mountSecret = Request.GetToken();

        if (!publicToken.VerifySecret(mountSecret)) {
            if (publicToken.Format != TMountToken::EFormat::EMPTY) {
                TMountToken current;

                current.SetSecret(
                    publicToken.Format,
                    mountSecret,
                    publicToken.Salt);

                LOG_WARN(ctx, TBlockStoreComponents::SERVICE,
                    "Invalid mount secret: %s. Expected: %s",
                    current.ToString().data(),
                    publicToken.ToString().data());
            } else {
                LOG_WARN(ctx, TBlockStoreComponents::SERVICE,
                    "Invalid mount secret. Expected empty secret");
            }

            auto error = MakeError(E_ARGUMENT, "Mount token verification failed");
            HandleDescribeVolumeError(ctx, error);
            return;
        }
    } else if (Request.GetToken()) {
        // Requests coming from control plane are authorized in Auth component
        // and so cannot also have mount token.
        auto error = MakeError(
            E_ARGUMENT,
            "Mount token is prohibited for authorized mount request");
        HandleDescribeVolumeError(ctx, error);
        return;
    }

    const auto& volumeConfig = volumeDescription.GetVolumeConfig();
    VolumeTabletId = volumeDescription.GetVolumeTabletId();

    if (VolumeTabletId != Params.KnownTabletId) {
        HandleDescribeVolumeError(
            ctx,
            MakeError(E_REJECTED, "Tablet id changed. Retry"));
        return;
    }

    const auto& requestEncryption = Request.GetEncryptionSpec();
    const auto& volumeEncryption = volumeConfig.GetEncryptionDesc();

    // if request has encryption mode then check volume encryption
    if (requestEncryption.GetMode() != NProto::NO_ENCRYPTION) {
        auto requestMode = static_cast<ui32>(requestEncryption.GetMode());
        if (requestMode != volumeEncryption.GetMode()) {
            HandleDescribeVolumeError(ctx, MakeError(E_ARGUMENT, TStringBuilder()
                << "Different encryption modes"
                << " in request (" << requestMode << ")"
                << " and in volume (" << volumeEncryption.GetMode() << ")"));
            return;
        }

        const auto& requestKeyHash = requestEncryption.GetKeyHash();
        if (NeedToSetEncryptionKeyHash(volumeConfig, requestKeyHash)) {
            const auto& clientId = Request.GetHeaders().GetClientId();
            auto request = std::make_unique<TEvService::TEvAlterVolumeRequest>();
            request->Record.MutableHeaders()->SetClientId(clientId);
            request->Record.SetDiskId(volumeConfig.GetDiskId());
            request->Record.SetProjectId(volumeConfig.GetProjectId());
            request->Record.SetFolderId(volumeConfig.GetFolderId());
            request->Record.SetCloudId(volumeConfig.GetCloudId());
            request->Record.SetConfigVersion(volumeConfig.GetVersion());
            request->Record.SetEncryptionKeyHash(requestKeyHash);

            NCloud::Send(ctx, MakeStorageServiceId(), std::move(request));
            return;
        }

        if (volumeEncryption.GetKeyHash() != requestKeyHash) {
            HandleDescribeVolumeError(ctx, MakeError(E_ARGUMENT, TStringBuilder()
                << "Different encryption key hashes"
                << " in request (" << requestKeyHash << ")"
                << " and in volume (" << volumeEncryption.GetKeyHash() << ")"));
            return;
        }
    }

    AddClient(ctx, Params.InitialAddClientTimeout);
}

////////////////////////////////////////////////////////////////////////////////

void TMountRequestActor::HandleDescribeVolumeError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    const auto& diskId = Request.GetDiskId();

    LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
        "Describe request failed for volume %s: %s",
        diskId.Quote().data(),
        FormatError(error).data());

    Error = error;

    if (Params.IsLocalMounter) {
        RequestVolumeStop(ctx);
    } else {
        NotifyAndDie(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TMountRequestActor::HandleAlterVolumeResponse(
    const TEvService::TEvAlterVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        HandleAlterVolumeError(ctx, error);
        return;
    }

    AddClient(ctx, Params.InitialAddClientTimeout);
}

void TMountRequestActor::HandleAlterVolumeError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    const auto& diskId = Request.GetDiskId();

    LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
        "Alter request failed for volume %s: %s",
        diskId.Quote().data(),
        FormatError(error).data());

    Error = error;

    if (Params.IsLocalMounter) {
        RequestVolumeStop(ctx);
    } else {
        NotifyAndDie(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TMountRequestActor::AddClient(const TActorContext& ctx, TDuration timeout)
{
    const auto& diskId = Request.GetDiskId();
    const auto& clientId = Request.GetHeaders().GetClientId();
    const auto& instanceId = Request.GetInstanceId();
    const auto accessMode = Request.GetVolumeAccessMode();
    const auto mountMode = Request.GetVolumeMountMode();
    const auto mountSeqNumber = Request.GetMountSeqNumber();
    const auto fillSeqNumber = Request.GetFillSeqNumber();
    const auto fillGeneration = Request.GetFillGeneration();

    auto request = std::make_unique<TEvVolume::TEvAddClientRequest>();
    request->Record.MutableHeaders()->SetClientId(clientId);
    request->Record.SetDiskId(diskId);
    request->Record.SetInstanceId(instanceId);
    request->Record.SetVolumeAccessMode(accessMode);
    request->Record.SetVolumeMountMode(mountMode);
    request->Record.SetMountFlags(Request.GetMountFlags());
    request->Record.SetMountSeqNumber(mountSeqNumber);
    request->Record.SetFillSeqNumber(fillSeqNumber);
    request->Record.SetFillGeneration(fillGeneration);
    request->Record.SetHost(FQDNHostName());

    auto requestInfo = CreateRequestInfo(
        SelfId(),
        RequestInfo->Cookie,
        RequestInfo->CallContext);

    NCloud::Register(ctx, CreateAddClientActor(
        std::move(request),
        std::move(requestInfo),
        timeout,
        Config->GetAddClientRetryTimeoutIncrement(),
        Params.VolumeClient));
}

////////////////////////////////////////////////////////////////////////////////

void TMountRequestActor::WaitForVolume(const TActorContext& ctx, TDuration timeout)
{
    const auto& diskId = Request.GetDiskId();
    const auto& clientId = Request.GetHeaders().GetClientId();

    auto request = std::make_unique<TEvVolume::TEvWaitReadyRequest>();
    request->Record.MutableHeaders()->SetClientId(clientId);
    request->Record.SetDiskId(diskId);

    auto requestInfo = CreateRequestInfo(
        SelfId(),
        RequestInfo->Cookie,
        RequestInfo->CallContext);

    NCloud::Register(ctx, CreateWaitReadyActor(
        std::move(request),
        std::move(requestInfo),
        timeout,
        Config->GetAddClientRetryTimeoutIncrement(),
        Params.VolumeClient));
}

////////////////////////////////////////////////////////////////////////////////

void TMountRequestActor::RequestVolumeStart(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvServicePrivate::TEvStartVolumeRequest>(
        VolumeTabletId);

    NCloud::Send(ctx, Params.SessionActorId, std::move(request));
}

void TMountRequestActor::RequestVolumeStop(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvServicePrivate::TEvStopVolumeRequest>();

    NCloud::Send(ctx, Params.SessionActorId, std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

void TMountRequestActor::LockVolume(const TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "[%lu] Acquiring tablet lock",
        VolumeTabletId);

    NCloud::Send<TEvHiveProxy::TEvLockTabletRequest>(
        ctx,
        MakeHiveProxyServiceId(),
        0,  // cookie
        VolumeTabletId);
}

void TMountRequestActor::UnlockVolume(const TActorContext& ctx)
{
    NCloud::Send<TEvHiveProxy::TEvUnlockTabletRequest>(
        ctx,
        MakeHiveProxyServiceId(),
        0,  // cookie
        VolumeTabletId);
}

////////////////////////////////////////////////////////////////////////////////

void TMountRequestActor::NotifyAndDie(const TActorContext& ctx)
{
    if (MountOptionsChanged && Error.GetCode() == S_ALREADY) {
        Error = {};
    }

    using TNotification = TEvServicePrivate::TEvMountRequestProcessed;
    auto notification = std::make_unique<TNotification>(
        Error,
        Volume,
        std::move(Request),
        Params.MountStartTick,
        RequestInfo,
        VolumeTabletId,
        VolumeStarted,
        Params.BindingType,
        Params.PreemptionSource,
        VolumeSessionRestartRequired);

    NCloud::Send(ctx, Params.SessionActorId, std::move(notification));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TMountRequestActor::HandleVolumeAddClientResponse(
    const TEvVolume::TEvAddClientResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    Error = error;
    Volume = msg->Record.GetVolume();

    if (SUCCEEDED(error.GetCode())) {
        AddClientRequestCompleted = true;

        if (msg->Record.GetForceTabletRestart() &&
            MountMode == NProto::VOLUME_MOUNT_LOCAL)
        {
            IsVolumeRestarting = true;
            RequestVolumeStop(ctx);
            return;
        }

        if (!VolumeStarted && MountMode == NProto::VOLUME_MOUNT_LOCAL) {
            RequestVolumeStart(ctx);
            return;
        }

        const bool volumeStarted = Params.StartVolumeActor || VolumeStarted;

        // the following expression describes the situation when local mounter
        // mounts volume for the first time but is forced to start volume remotely.
        const bool newPreemptedLocalMounter =
            Request.GetVolumeMountMode() == NProto::VOLUME_MOUNT_LOCAL &&
            MountMode == NProto::VOLUME_MOUNT_REMOTE &&
            !Params.IsLocalMounter;

        // NBS-3481
        // we should acquire lock for tablet in hive if it is not already running
        // at host and local mount cannot be satisfied because of remote binding.
        // if we don't do this, tablet will stay at previous local mount host.
        if (!IsTabletAcquired && !volumeStarted && newPreemptedLocalMounter) {
            LockVolume(ctx);
            return;
        }

        const bool mayStopVolume = Params.IsLocalMounter || VolumeStarted;

        if (mayStopVolume && MountMode == NProto::VOLUME_MOUNT_REMOTE) {
            RequestVolumeStop(ctx);
            return;
        }
    } else if (VolumeStarted || error.GetCode() == E_BS_MOUNT_CONFLICT) {
        RequestVolumeStop(ctx);
        return;
    } else if (error.GetCode() == E_REJECTED) {
        NCloud::Send<TEvServicePrivate::TEvResetPipeClient>(ctx, Params.VolumeClient);

        if (!Params.RejectOnAddClientTimeout) {
            RequestVolumeStart(ctx);
            return;
        }
    }

    NotifyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TMountRequestActor::HandleWaitReadyResponse(
    const TEvVolume::TEvWaitReadyResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Error = ev->Get()->Record.GetError();
    NotifyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TMountRequestActor::HandleLockTabletResponse(
    const TEvHiveProxy::TEvLockTabletResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "[%lu] Failed to acquire tablet lock: %s",
            VolumeTabletId,
            FormatError(error).data());

        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "[%lu] Successfully acquired tablet lock",
        VolumeTabletId);

    // NBS-3481 - unlock this volume to release it to hive to be sure
    // that during migration this volume will not get stuck at the source node
    UnlockVolume(ctx);
}

void TMountRequestActor::HandleUnlockTabletResponse(
    const TEvHiveProxy::TEvUnlockTabletResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "[%lu] Tablet lock has been released",
        VolumeTabletId);

    IsTabletAcquired = true;

    AddClient(ctx, Config->GetLocalStartAddClientTimeout());
}

////////////////////////////////////////////////////////////////////////////////

void TMountRequestActor::HandleStartVolumeResponse(
    const TEvServicePrivate::TEvStartVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& msg = ev->Get();
    const auto& error = msg->GetError();
    const auto& mountMode = Request.GetVolumeMountMode();

    IsVolumeRestarting = false;

    if (!AddClientRequestCompleted) {
        Volume = msg->VolumeInfo;
    }

    Error = error;
    if (SUCCEEDED(error.GetCode())) {
        VolumeStarted = true;
        if (AddClientRequestCompleted && mountMode == NProto::VOLUME_MOUNT_LOCAL) {
            NotifyAndDie(ctx);
            return;
        }
    } else {
        if (mountMode == NProto::VOLUME_MOUNT_LOCAL) {
            VolumeSessionRestartRequired = true;
            NotifyAndDie(ctx);
            return;
        }
    }

    IsTabletAcquired = true;

    AddClient(ctx, Config->GetLocalStartAddClientTimeout());
}

void TMountRequestActor::HandleStopVolumeResponse(
    const TEvServicePrivate::TEvStopVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    IsTabletAcquired = false;

    if (!FAILED(Error.GetCode())) {
        if (IsVolumeRestarting) {
            RequestVolumeStart(ctx);
            return;
        }
        WaitForVolume(ctx, Config->GetLocalStartAddClientTimeout());
        return;
    }

    NotifyAndDie(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TMountRequestActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSSProxy::TEvDescribeVolumeResponse, HandleDescribeVolumeResponse);
        HFunc(TEvService::TEvAlterVolumeResponse, HandleAlterVolumeResponse);

        HFunc(TEvVolume::TEvAddClientResponse, HandleVolumeAddClientResponse);
        HFunc(TEvVolume::TEvWaitReadyResponse, HandleWaitReadyResponse);

        HFunc(TEvServicePrivate::TEvStartVolumeResponse, HandleStartVolumeResponse);
        HFunc(TEvServicePrivate::TEvStopVolumeResponse, HandleStopVolumeResponse);

        HFunc(TEvHiveProxy::TEvUnlockTabletResponse, HandleUnlockTabletResponse);
        HFunc(TEvHiveProxy::TEvLockTabletResponse, HandleLockTabletResponse);

        IgnoreFunc(TEvHiveProxy::TEvTabletLockLost);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::SERVICE,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TVolumeSessionActor::LogNewClient(
    const TActorContext& ctx,
    const TEvServicePrivate::TEvInternalMountVolumeRequest::TPtr& ev,
    ui64 tick)
{
    const auto* msg = ev->Get();
    const auto& diskId = msg->Record.GetDiskId();
    const auto& clientId = msg->Record.GetHeaders().GetClientId();
    const auto& instanceId = msg->Record.GetInstanceId();
    const auto& accessMode = msg->Record.GetVolumeAccessMode();
    const auto& mountMode = msg->Record.GetVolumeMountMode();
    const auto& mountSeqNumber = msg->Record.GetMountSeqNumber();

    TStringBuf throttlingStr =
        IsThrottlingDisabled(msg->Record) ? "disabled" : "enabled";

    LOG_INFO_S(ctx, TBlockStoreComponents::SERVICE,
        "Mounting volume: " << diskId.Quote() <<
        " (client: " << clientId.Quote() <<
        " instance: " << instanceId.Quote() <<
        " access: " << AccessModeToString(accessMode) <<
        " mount mode: " << EVolumeMountMode_Name(mountMode) <<
        " throttling: " << throttlingStr <<
        " seq_num: " << mountSeqNumber <<
        " binding: " << EVolumeBinding_Name(msg->BindingType) <<
        " source: " << EPreemptionSource_Name(msg->PreemptionSource) <<
        " cur binding: " << EVolumeBinding_Name(VolumeInfo->BindingType) <<
        " cur source: " << EPreemptionSource_Name(VolumeInfo->PreemptionSource) <<
        ") ts: " << tick);
}

TVolumeSessionActor::TMountRequestProcResult TVolumeSessionActor::ProcessMountRequest(
    const TActorContext& ctx,
    const TEvServicePrivate::TEvInternalMountVolumeRequest::TPtr& ev,
    ui64 tick)
{
    const auto* msg = ev->Get();
    const auto& diskId = msg->Record.GetDiskId();
    const auto& clientId = msg->Record.GetHeaders().GetClientId();
    const auto& accessMode = msg->Record.GetVolumeAccessMode();
    const auto& mountMode = msg->Record.GetVolumeMountMode();
    const auto& mountSeqNumber = msg->Record.GetMountSeqNumber();
    const auto& encryptionKeyHash = msg->Record.GetEncryptionSpec().GetKeyHash();
    const auto& fillSeqNumber = msg->Record.GetFillSeqNumber();
    const auto& fillGeneration = msg->Record.GetFillGeneration();

    auto* clientInfo = VolumeInfo->GetClientInfo(clientId);
    if (!clientInfo) {
        LogNewClient(ctx, ev, tick);
        return {{}, false};
    }

    TStringBuf throttlingStr =
        IsThrottlingDisabled(msg->Record) ? "disabled" : "enabled";

    TStringBuilder mountParamsStr =
        TStringBuilder() <<
        " (client: " << clientId.Quote() <<
        " access: " << AccessModeToString(accessMode) <<
        " mount mode: " << EVolumeMountMode_Name(mountMode) <<
        " throttling: " << throttlingStr <<
        " seq_num: " << mountSeqNumber <<
        " binding: " << EVolumeBinding_Name(msg->BindingType) <<
        " source: " << EPreemptionSource_Name(msg->PreemptionSource) <<
        " cur binding: " << EVolumeBinding_Name(VolumeInfo->BindingType) <<
        " cur source: " << EPreemptionSource_Name(VolumeInfo->PreemptionSource) <<
        " key_hash: " << encryptionKeyHash.Quote() <<
        ") ts: " << tick;

    LOG_DEBUG_S(ctx, TBlockStoreComponents::SERVICE,
        "Mounting volume: " << diskId.Quote() << mountParamsStr);

    // TODO: check for duplicate requests
    auto prevActivityTime = clientInfo->LastActivityTime;
    clientInfo->LastActivityTime = ctx.Now();

    const auto& volume = VolumeInfo->VolumeInfo;
    if (!volume || NeedToSetEncryptionKeyHash(*volume, encryptionKeyHash)) {
        // Need to set encryption KeyHash in volume config
        return {{}, true};
    }

    if (VolumeInfo->State == TVolumeInfo::INITIAL &&
        mountMode == NProto::VOLUME_MOUNT_LOCAL &&
        VolumeInfo->BindingType != NProto::BINDING_REMOTE)
    {
        // Volume tablet is not started but needs to be
        return {{}, false};
    }

    bool mountOptionsChanged = clientInfo->VolumeMountMode != mountMode;

    if (mountOptionsChanged) {
        LOG_INFO_S(ctx, TBlockStoreComponents::SERVICE,
            "Re-mounting volume with new options: " << diskId.Quote()
            << mountParamsStr);

        return {{}, true};
    }

    if (mountMode == NProto::VOLUME_MOUNT_LOCAL &&
        !StartVolumeActor &&
        VolumeInfo->BindingType != NProto::BINDING_REMOTE)
    {
        // Remount for locally mounted volume and volume has gone.
        // Need to return tablet back.
        return {{}, false};
    }

    if (mountSeqNumber != clientInfo->MountSeqNumber ||
        fillSeqNumber != clientInfo->FillSeqNumber ||
        fillGeneration != clientInfo->FillGeneration)
    {
        // If mountSeqNumber, fillSeqNumber or fillGeneration
        // has changed in MountVolumeRequest from client
        // then let volume know about this
        return {{}, false};
    }

    if (clientInfo->LastMountTick < LastPipeResetTick) {
        // Pipe reset happened after last execution of AddClient
        // we need to run MountActor again and send AddClient
        // to update client information at volume.
        return {{}, false};
    }

    if (clientInfo->VolumeAccessMode != accessMode) {
        return {{}, false};
    }

    auto now = ctx.Now();
    auto remountPeriod = Config->GetClientRemountPeriod();
    if (prevActivityTime &&
        (now - prevActivityTime) > remountPeriod + RemountDelayWarn)
    {
        LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
            "Late ping from client %s for volume %s, no activity for %s",
            clientId.Quote().data(),
            diskId.Quote().data(),
            ToString(remountPeriod + RemountDelayWarn).data());
    }

    return {MakeError(S_ALREADY, "Volume already mounted"), false};
}

template <typename TProtoRequest>
void TVolumeSessionActor::AddClientToVolume(
    const TActorContext& ctx,
    const TProtoRequest& mountRequest,
    ui64 mountTick)
{
    auto* clientInfo = VolumeInfo->AddClientInfo(mountRequest.GetHeaders().GetClientId());
    clientInfo->VolumeAccessMode = mountRequest.GetVolumeAccessMode();
    clientInfo->VolumeMountMode = mountRequest.GetVolumeMountMode();
    clientInfo->MountFlags = mountRequest.GetMountFlags();
    clientInfo->IpcType = mountRequest.GetIpcType();
    clientInfo->LastActivityTime = ctx.Now();
    clientInfo->LastMountTick = mountTick;
    clientInfo->ClientVersionInfo = std::move(mountRequest.GetClientVersionInfo());
    clientInfo->MountSeqNumber = mountRequest.GetMountSeqNumber();
    clientInfo->FillSeqNumber = mountRequest.GetFillSeqNumber();
    clientInfo->FillGeneration = mountRequest.GetFillGeneration();
}

void TVolumeSessionActor::HandleInternalMountVolume(
    const TEvServicePrivate::TEvInternalMountVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (ShuttingDown) {
        SendInternalMountVolumeResponse(
            ctx,
            ev,
            ShuttingDownError,
            *VolumeInfo,
            *Config);
        return;
    }

    auto* msg = ev->Get();
    const auto& diskId = msg->Record.GetDiskId();
    const auto& clientId = msg->Record.GetHeaders().GetClientId();

    if (MountRequestActor || UnmountRequestActor) {
        LOG_INFO_S(ctx, TBlockStoreComponents::SERVICE,
            "Queuing mount volume " << diskId.Quote() <<
            " by client " << clientId.Quote() <<
            " request because of active " <<
            (MountRequestActor ? "mount" : "unmount") << " request");

        MountUnmountRequests.emplace(ev.Release());
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    auto tick = GetCycleCount();
    auto procResult = ProcessMountRequest(ctx, ev, tick);
    auto error = std::move(procResult.Error);

    auto bindingType = msg->BindingType;
    bool isBindingChanging = msg->PreemptionSource != NProto::SOURCE_NONE;

    bool shouldReply = false;

    if (error.Defined() && FAILED(error->GetCode())) {
        shouldReply = true;
    }

    TClientInfo* clientInfo = nullptr;

    if (!shouldReply) {
        clientInfo = VolumeInfo->GetClientInfo(clientId);
        if (isBindingChanging &&
            (!clientInfo || VolumeInfo->GetLocalMountClientInfo() != clientInfo))
        {
            error = MakeError(E_ARGUMENT, "No local mounter found");
            shouldReply = true;
        } else {
            bindingType = VolumeInfo->OnMountStarted(
                *SharedCounters,
                msg->PreemptionSource,
                msg->BindingType,
                msg->Record.GetVolumeMountMode(),
                !IsDiskRegistryMediaKind(VolumeInfo->StorageMediaKind));
            shouldReply =
                (bindingType == VolumeInfo->BindingType) &&
                clientInfo &&
                !procResult.MountOptionsChanged &&
                error.Defined();
            if (shouldReply) {
                VolumeInfo->OnMountCancelled(
                    *SharedCounters,
                    msg->PreemptionSource);

                if (VolumeInfo->ShouldSyncManuallyPreemptedVolumes()) {
                    SendUpdateManuallyPreemptedVolumes(
                        ctx,
                        diskId,
                        VolumeInfo->PreemptionSource);
                }
            }
        }
    }

    if (shouldReply) {
        SendInternalMountVolumeResponse(
            ctx,
            requestInfo,
            *error,
            *VolumeInfo,
            *Config);
        ReceiveNextMountOrUnmountRequest(ctx);
        return;
    }

    TDuration timeout = VolumeInfo->InitialAddClientTimeout;
    if (!timeout) {
        timeout = Config->GetInitialAddClientTimeout();
    }

    TMountRequestParams params {
        .MountStartTick = tick,
        .InitialAddClientTimeout = timeout,
        .SessionActorId = SelfId(),
        .VolumeClient = VolumeClient,
        .StartVolumeActor = StartVolumeActor,
        .BindingType = bindingType,
        .PreemptionSource = msg->PreemptionSource,
        .IsLocalMounter = clientInfo &&
            clientInfo->VolumeMountMode == NProto::VOLUME_MOUNT_LOCAL,
        .RejectOnAddClientTimeout = Config->GetRejectMountOnAddClientTimeout(),
        .KnownTabletId = TabletId};

    MountRequestActor = NCloud::Register<TMountRequestActor>(
        ctx,
        Config,
        std::move(requestInfo),
        std::move(msg->Record),
        VolumeInfo->SessionId,
        params,
        procResult.MountOptionsChanged);
}

void TVolumeSessionActor::PostponeMountVolume(
    const TEvServicePrivate::TEvInternalMountVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (ShuttingDown) {
        const auto requestInfo = CreateRequestInfo(
            ev->Sender,
            ev->Cookie,
            ev->Get()->CallContext);

        SendInternalMountVolumeResponse(
            ctx,
            requestInfo,
            ShuttingDownError,
            *VolumeInfo,
            *Config);
        return;
    }

    MountUnmountRequests.emplace(ev.Release());
}

void TVolumeSessionActor::HandleMountRequestProcessed(
    const TEvServicePrivate::TEvMountRequestProcessed::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    auto& mountRequest = msg->Request;
    const auto& diskId = mountRequest.GetDiskId();
    const auto& clientId = mountRequest.GetHeaders().GetClientId();
    const auto mountStartTick = msg->MountStartTick;
    const bool hadLocalStart = msg->HadLocalStart;

    VolumeInfo->TabletId = msg->VolumeTabletId;

    TStringBuilder mountStr =
        TStringBuilder() <<
        "Mount completed for client " << clientId.Quote() <<
        " , volume " << diskId.Quote() <<
        " , msg binding " << EVolumeBinding_Name(msg->BindingType) <<
        " , msg source " << EPreemptionSource_Name(msg->PreemptionSource) <<
        " , time " << ToString(mountStartTick).Quote();

    if (HasError(msg->GetError())) {
        VolumeInfo->RemoveClientInfo(clientId);
        VolumeInfo->OnMountFinished(
            *SharedCounters,
            msg->PreemptionSource,
            msg->BindingType,
            msg->GetError());

        if (VolumeInfo->ShouldSyncManuallyPreemptedVolumes()) {
            SendUpdateManuallyPreemptedVolumes(
                ctx,
                diskId,
                VolumeInfo->PreemptionSource);
        }

        LOG_INFO_S(ctx, TBlockStoreComponents::SERVICE,
            mountStr <<
            " , binding " << EVolumeBinding_Name(VolumeInfo->BindingType) <<
            " , source " << EPreemptionSource_Name(VolumeInfo->PreemptionSource) <<
            " , result (" << msg->GetStatus() <<
            "): " << msg->GetError().GetMessage().Quote());

        SendInternalMountVolumeResponse(
            ctx,
            msg->RequestInfo,
            msg->GetError(),
            *VolumeInfo,
            *Config);

        MountRequestActor = {};

        if (msg->VolumeSessionRestartRequired) {
            FailPendingRequestsAndDie(
                ctx,
                MakeError(E_REJECTED, "Disk tablet possibly changed. Retry"));
            return;
        }

        if (!MountUnmountRequests.empty()) {
            ReceiveNextMountOrUnmountRequest(ctx);
        } else if (!VolumeInfo->IsMounted()) {
            NotifyAndDie(ctx);
        }
        return;
    }

    VolumeInfo->VolumeInfo = msg->Volume;

    AddClientToVolume(
        ctx,
        mountRequest,
        mountStartTick);

    VolumeInfo->OnMountFinished(
        *SharedCounters,
        msg->PreemptionSource,
        msg->BindingType,
        msg->GetError());

    LOG_INFO_S(ctx, TBlockStoreComponents::SERVICE,
        mountStr <<
        " , binding " << EVolumeBinding_Name(VolumeInfo->BindingType) <<
        " , source " << EPreemptionSource_Name(VolumeInfo->PreemptionSource));

    if (VolumeInfo->ShouldSyncManuallyPreemptedVolumes()) {
        SendUpdateManuallyPreemptedVolumes(
            ctx,
            diskId,
            VolumeInfo->PreemptionSource);
    }

    if (hadLocalStart) {
        VolumeInfo->InitialAddClientTimeout =
            Config->GetInitialAddClientTimeout() * InitialAddClientMultiplier;
    }

    SendInternalMountVolumeResponse(
        ctx,
        msg->RequestInfo,
        msg->GetError(),
        *VolumeInfo,
        *Config);

    ScheduleInactiveClientsRemoval(ctx);

    MountRequestActor = {};
    ReceiveNextMountOrUnmountRequest(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
