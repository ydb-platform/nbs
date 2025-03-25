#include "service_actor.h"

#include <cloud/blockstore/libs/encryption/encryption_key.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/disk_validation.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/core/volume_model.h>
#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/common/media.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/size_literals.h>
#include <util/string/ascii.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCreateVolumeActor final
    : public TActorBootstrapped<TCreateVolumeActor>
{
private:
    const TRequestInfoPtr RequestInfo;

    const TStorageConfigPtr Config;
    const NProto::TCreateVolumeRequest Request;
    const IRootKmsKeyProviderPtr KeyProvider;

    ui64 BaseDiskTabletId = 0;

public:
    TCreateVolumeActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        NProto::TCreateVolumeRequest request,
        IRootKmsKeyProviderPtr keyProvider);

    void Bootstrap(const TActorContext& ctx);

private:
    ui32 GetBlockSize() const;
    NCloud::NProto::EStorageMediaKind GetStorageMediaKind() const;
    bool ShouldCreateVolumeWithEncryptionAtRest() const;

    void DescribeBaseVolume(const TActorContext& ctx);
    void CreateVolume(const TActorContext& ctx);
    void CreateVolumeImpl(
        const TActorContext& ctx,
        NKikimrBlockStore::TEncryptionDesc encryptionDesc);

    void HandleDescribeVolumeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleCreateVolumeResponse(
        const TEvSSProxy::TEvCreateVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleWaitReadyResponse(
        const TEvVolume::TEvWaitReadyResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleCreateEncryptionKeyResponse(
        const TEvServicePrivate::TEvCreateEncryptionKeyResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvCreateVolumeResponse> response);

private:
    STFUNC(StateWork);
};

////////////////////////////////////////////////////////////////////////////////

TCreateVolumeActor::TCreateVolumeActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        NProto::TCreateVolumeRequest request,
        IRootKmsKeyProviderPtr keyProvider)
    : RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , Request(std::move(request))
    , KeyProvider(std::move(keyProvider))
{}

void TCreateVolumeActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    DescribeBaseVolume(ctx);
}

ui32 TCreateVolumeActor::GetBlockSize() const
{
    ui32 blockSize = Request.GetBlockSize();
    if (!blockSize) {
        blockSize = DefaultBlockSize;
    }
    return blockSize;
}

NCloud::NProto::EStorageMediaKind TCreateVolumeActor::GetStorageMediaKind() const
{
    switch (Request.GetStorageMediaKind()) {
        case NCloud::NProto::STORAGE_MEDIA_DEFAULT:
            return NCloud::NProto::STORAGE_MEDIA_HDD;
        default:
            return Request.GetStorageMediaKind();
    }
}

void TCreateVolumeActor::DescribeBaseVolume(const TActorContext& ctx)
{
    const auto& baseDiskId = Request.GetBaseDiskId();

    if (baseDiskId) {
        NCloud::Send(
            ctx,
            MakeSSProxyServiceId(),
            std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(baseDiskId));
    } else {
        CreateVolume(ctx);
    }
}

bool TCreateVolumeActor::ShouldCreateVolumeWithEncryptionAtRest() const
{
    if (IsDiskRegistryLocalMediaKind(GetStorageMediaKind())) {
        // Encryption at rest for local disks is not implemented (#2598)
        return false;
    }

    return IsDiskRegistryMediaKind(GetStorageMediaKind()) &&
            // Direct request for encryption at rest
           (Request.GetEncryptionSpec().GetMode() ==
                NProto::ENCRYPTION_AT_REST ||
            // Client did not request encryption with the provided key
            (Request.GetEncryptionSpec().GetMode() == NProto::NO_ENCRYPTION &&
            // and the feature is enabled for the disk/cloud/folder
             (Config->GetEncryptionAtRestForDiskRegistryBasedDisksEnabled() ||
              Config->IsEncryptionAtRestForDiskRegistryBasedDisksFeatureEnabled(
                  Request.GetCloudId(),
                  Request.GetFolderId(),
                  Request.GetDiskId()))));
}

void TCreateVolumeActor::CreateVolume(const TActorContext& ctx)
{
    if (ShouldCreateVolumeWithEncryptionAtRest()) {
        if (!Request.GetBaseDiskId().empty()) {
            ReplyAndDie(
                ctx,
                std::make_unique<TEvService::TEvCreateVolumeResponse>(MakeError(
                    E_NOT_IMPLEMENTED,
                    "Encrypted overlay disks are not supported")));
            return;
        }

        LOG_INFO_S(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Generate DEK for " << Request.GetDiskId().Quote());

        KeyProvider->GenerateDataEncryptionKey(Request.GetDiskId())
            .Subscribe(
                [actorSystem = TActivationContext::ActorSystem(),
                 selfId = ctx.SelfID](const auto& future)
                {
                    const auto& [key, error] = future.GetValue();

                    auto response = std::make_unique<
                        TEvServicePrivate::TEvCreateEncryptionKeyResponse>(
                        error,
                        key);

                    actorSystem->Send(selfId, response.release());
                });
        return;
    }

    NKikimrBlockStore::TEncryptionDesc encryptionDesc;

    const auto& encryptionSpec = Request.GetEncryptionSpec();
    if (encryptionSpec.GetMode() != NProto::NO_ENCRYPTION) {
        encryptionDesc.SetMode(encryptionSpec.GetMode());
        encryptionDesc.SetKeyHash(encryptionSpec.GetKeyHash());
    }

    CreateVolumeImpl(ctx, std::move(encryptionDesc));
}

void TCreateVolumeActor::CreateVolumeImpl(
    const TActorContext& ctx,
    NKikimrBlockStore::TEncryptionDesc encryptionDesc)
{
    NKikimrBlockStore::TVolumeConfig config;

    config.SetBlockSize(GetBlockSize());
    const auto maxBlocksInBlob = CalculateMaxBlocksInBlob(
        Config->GetMaxBlobSize(),
        GetBlockSize()
    );
    if (maxBlocksInBlob != MaxBlocksCount) {
        // MaxBlocksInBlob is not equal to the default value
        // => it needs to be stored
        config.SetMaxBlocksInBlob(maxBlocksInBlob);
    }
    config.SetZoneBlockCount(Config->GetZoneBlockCount());
    config.SetDiskId(Request.GetDiskId());
    config.SetFolderId(Request.GetFolderId());
    config.SetCloudId(Request.GetCloudId());
    config.SetProjectId(Request.GetProjectId());
    config.SetTabletVersion(
        Request.GetTabletVersion()
        ? Request.GetTabletVersion()
        : Config->GetDefaultTabletVersion()
    );
    config.SetStorageMediaKind(GetStorageMediaKind());
    config.SetBaseDiskId(Request.GetBaseDiskId());
    config.SetBaseDiskTabletId(BaseDiskTabletId);
    config.SetBaseDiskCheckpointId(Request.GetBaseDiskCheckpointId());
    config.SetIsSystem(Request.GetIsSystem());
    config.SetFillGeneration(Request.GetFillGeneration());

    {
        const TVolumeParams volumeParams = CreateVolumeParams(
            *Config,
            TCreateVolumeParamsCtx{
                .BlockSize = GetBlockSize(),
                .BlocksCount = Request.GetBlocksCount(),
                .MediaKind = GetStorageMediaKind(),
                .PartitionsCount = Request.GetPartitionsCount(),
                .CloudId = Request.GetCloudId(),
                .FolderId = Request.GetFolderId(),
                .DiskId = Request.GetDiskId(),
                .IsSystem = Request.GetIsSystem(),
                .IsOverlayDisk = !Request.GetBaseDiskId().empty()});

        if (volumeParams.PartitionsCount > 1) {
            config.SetBlocksPerStripe(ceil(
                static_cast<double>(Config->GetBytesPerStripe()) /
                volumeParams.BlockSize));
        }

        ResizeVolume(
            *Config,
            volumeParams,
            {},
            Request.GetPerformanceProfile(),
            config);
    }
    config.SetCreationTs(ctx.Now().MicroSeconds());

    config.SetPlacementGroupId(Request.GetPlacementGroupId());
    config.SetPlacementPartitionIndex(Request.GetPlacementPartitionIndex());
    if (Request.GetStoragePoolName()) {
        config.SetStoragePoolName(Request.GetStoragePoolName());
    } else if (GetStorageMediaKind() == NProto::STORAGE_MEDIA_HDD_NONREPLICATED)
    {
        config.SetStoragePoolName(Config->GetNonReplicatedHDDPoolName());
    }
    config.MutableAgentIds()->CopyFrom(Request.GetAgentIds());

    if (encryptionDesc.GetMode() != NProto::NO_ENCRYPTION) {
        LOG_DEBUG_S(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Creating volume with an encryption: "
                << NProto::EEncryptionMode_Name(encryptionDesc.GetMode()));

        *config.MutableEncryptionDesc() = std::move(encryptionDesc);
    }

    auto request = std::make_unique<TEvSSProxy::TEvCreateVolumeRequest>(
        std::move(config));

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Sending createvolume request for volume %s",
        Request.GetDiskId().Quote().c_str());

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TCreateVolumeActor::HandleCreateEncryptionKeyResponse(
    const TEvServicePrivate::TEvCreateEncryptionKeyResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& msg = *ev->Get();

    if (const auto& error = msg.GetError(); HasError(error)) {
        LOG_ERROR_S(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Failed to generate encryption key: " << FormatError(error));
        ReplyAndDie(
            ctx,
            std::make_unique<TEvService::TEvCreateVolumeResponse>(error));

        return;
    }

    LOG_INFO_S(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Create volume " << Request.GetDiskId().Quote()
                            << " with default AES XTS encryption");

    NKikimrBlockStore::TEncryptionDesc encryptionDesc;
    encryptionDesc.SetMode(NProto::ENCRYPTION_AT_REST);

    auto& dek = *encryptionDesc.MutableEncryptedDataKey();
    dek.SetKekId(msg.KmsKey.GetKekId());
    dek.SetCiphertext(msg.KmsKey.GetEncryptedDEK());

    CreateVolumeImpl(ctx, std::move(encryptionDesc));
}

void TCreateVolumeActor::HandleDescribeVolumeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& baseDiskId = Request.GetBaseDiskId();
    const auto& msg = ev->Get();
    const auto& error = msg->GetError();

    if (FAILED(error.GetCode())) {
        LOG_ERROR_S(ctx, TBlockStoreComponents::VOLUME,
            "Could not resolve path for base volume "
            << baseDiskId.Quote() << ": " << FormatError(error));

        ReplyAndDie(
            ctx,
            std::make_unique<TEvService::TEvCreateVolumeResponse>(error));

        return;

    }

    const auto& pathDescr = msg->PathDescription;
    const auto& volumeDescr = pathDescr.GetBlockStoreVolumeDescription();
    const auto& tabletId = volumeDescr.GetVolumeTabletId();

    LOG_INFO_S(ctx, TBlockStoreComponents::VOLUME, "Resolved base disk id "
        << baseDiskId.Quote() << " to tablet id " << tabletId);

    BaseDiskTabletId = tabletId;

    CreateVolume(ctx);
}

void TCreateVolumeActor::HandleCreateVolumeResponse(
    const TEvSSProxy::TEvCreateVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (HasError(error)) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "Creation of volume %s failed: %s",
            Request.GetDiskId().Quote().c_str(),
            msg->GetErrorReason().c_str());

        ReplyAndDie(
            ctx,
            std::make_unique<TEvService::TEvCreateVolumeResponse>(error));

        return;
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
        "Sending WaitReady request to volume %s",
        Request.GetDiskId().Quote().c_str());

    auto request = std::make_unique<TEvVolume::TEvWaitReadyRequest>();
    request->Record.SetDiskId(Request.GetDiskId());

    NCloud::Send(
        ctx,
        MakeVolumeProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TCreateVolumeActor::HandleWaitReadyResponse(
    const TEvVolume::TEvWaitReadyResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    auto error = msg->GetError();

    if (HasError(error)) {
        LOG_WARN(ctx, TBlockStoreComponents::SERVICE,
            "Volume %s creation failed with error: %s",
            Request.GetDiskId().Quote().c_str(),
            error.GetMessage().Quote().c_str());
    } else {
        LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE,
            "Successfully created volume %s",
            Request.GetDiskId().Quote().c_str());
    }

    ReplyAndDie(
        ctx,
        std::make_unique<TEvService::TEvCreateVolumeResponse>(std::move(error)));
}

void TCreateVolumeActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvCreateVolumeResponse> response)
{
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TCreateVolumeActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvSSProxy::TEvCreateVolumeResponse, HandleCreateVolumeResponse);
        HFunc(TEvVolume::TEvWaitReadyResponse, HandleWaitReadyResponse);
        HFunc(
            TEvSSProxy::TEvDescribeVolumeResponse,
            HandleDescribeVolumeResponse);
        HFunc(
            TEvServicePrivate::TEvCreateEncryptionKeyResponse,
            HandleCreateEncryptionKeyResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::SERVICE);
            break;
    }
}

NProto::TError ValidateCreateVolumeRequest(
    const TStorageConfig& config,
    const NProto::TCreateVolumeRequest& request)
{
    TString errorMessage;

    if (!request.GetDiskId()) {
        return MakeError(E_ARGUMENT, "DiskId cannot be empty");
    }

    static const TStringBuf allowedChars = "_-@.";

    for (ui32 i = 0; i < request.GetDiskId().size(); ++i) {
        const auto c = request.GetDiskId()[i];
        if (!IsAsciiAlnum(c) && allowedChars.find(c) == TString::npos) {
            return MakeError(
                E_ARGUMENT,
                TStringBuilder() << "Bad character at pos "
                    << i << " in DiskId");
        }
    }

    if (request.GetTabletVersion() > MaxSupportedTabletVersion) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "bad tablet version: "
                << request.GetTabletVersion()
                << " < " << MaxSupportedTabletVersion);
    }

    if (request.GetBaseDiskId() && !request.GetBaseDiskCheckpointId()) {
        return MakeError(
            E_ARGUMENT,
            "BaseDiskCheckpointId cannot be empty for overlay disk");
    }

    if (request.GetPartitionsCount() > 1
            && (request.GetBaseDiskId() || request.GetIsSystem()))
    {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "base and overlay disks with "
                << request.GetPartitionsCount()
                << " partitions are not implemented");
    }

    if (request.GetPartitionsCount() > config.GetMaxPartitionsPerVolume()) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "too many partitions specified: "
                << request.GetPartitionsCount() << " > "
                << config.GetMaxPartitionsPerVolume());
    }

    const auto mediaKind = request.GetStorageMediaKind();
    const auto maxBlocks = ComputeMaxBlocks(config, mediaKind, 0);
    if (request.GetBlocksCount() > maxBlocks) {
        return MakeError(
            E_ARGUMENT,
            TStringBuilder() << "disk size for media kind "
                << static_cast<int>(mediaKind)
                << " should be <= " << maxBlocks << " blocks");
    }

    if (!request.GetBlocksCount()) {
        return MakeError(E_ARGUMENT, "disk size should not be 0");
    }

    const auto vbsError = ValidateBlockSize(request.GetBlockSize(), mediaKind);

    if (HasError(vbsError)) {
        return vbsError;
    }

    if (!IsDiskRegistryMediaKind(mediaKind)) {
        if (request.GetPlacementGroupId()) {
            return MakeError(
                E_ARGUMENT,
                "PlacementGroupId not allowed for replicated disks");
        }

        if (request.GetStoragePoolName()) {
            return MakeError(
                E_ARGUMENT,
                "StoragePoolName not allowed for replicated disks");
        }

        if (request.AgentIdsSize()) {
            return MakeError(
                E_ARGUMENT,
                "AgentIds not allowed for replicated disks");
        }
    } else {
        const ui64 volumeSize = request.GetBlockSize() * request.GetBlocksCount();
        const ui64 unit = GetAllocationUnit(config, mediaKind);

        if (volumeSize % unit != 0) {
            return MakeError(
                E_ARGUMENT, TStringBuilder()
                    << "volume size should be divisible by " << unit);
        }
    }

    const auto& encryptionSpec = request.GetEncryptionSpec();
    if (encryptionSpec.GetMode() != NProto::NO_ENCRYPTION &&
        encryptionSpec.HasKeyPath())
    {
        return MakeError(E_ARGUMENT, "KeyPath not supported in disk creation");
    }

    return MakeError(S_OK);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleCreateVolume(
    const TEvService::TEvCreateVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    auto request = msg->Record;

    const bool useNonReplicatedHdd =
        Config->IsUseNonReplicatedHDDInsteadOfReplicatedFeatureEnabled(
            request.GetCloudId(),
            request.GetFolderId(),
            request.GetDiskId());

    if (useNonReplicatedHdd) {
        switch (request.GetStorageMediaKind()) {
            case NCloud::NProto::STORAGE_MEDIA_DEFAULT:
            case NCloud::NProto::STORAGE_MEDIA_HDD:
            case NCloud::NProto::STORAGE_MEDIA_HYBRID: {
                const auto newMediaKind =
                    NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED;

                LOG_WARN(ctx, TBlockStoreComponents::SERVICE,
                    "Replaced media kind with %d for disk: %s",
                    int(newMediaKind),
                    request.GetDiskId().Quote().c_str());

                request.SetStorageMediaKind(newMediaKind);
                break;
            }
            default: break;
        }
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    const auto error = ValidateCreateVolumeRequest(*Config, request);
    if (HasError(error)) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "CreateVolumeRequest validation failed, volume %s error %s",
            request.GetDiskId().c_str(),
            FormatError(error).c_str());

        NCloud::Reply(
            ctx,
            *requestInfo,
            std::make_unique<TEvService::TEvCreateVolumeResponse>(error));

        return;
    }

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE,
        "Creating volume: %s, %s, %s, %s, %s, %u, %llu, %u, %d",
        request.GetDiskId().Quote().c_str(),
        request.GetProjectId().Quote().c_str(),
        request.GetFolderId().Quote().c_str(),
        request.GetCloudId().Quote().c_str(),
        request.GetPlacementGroupId().Quote().c_str(),
        request.GetPlacementPartitionIndex(),
        request.GetBlocksCount(),
        request.GetBlockSize(),
        int(request.GetStorageMediaKind()));

    NCloud::Register<TCreateVolumeActor>(
        ctx,
        std::move(requestInfo),
        Config,
        std::move(request),
        RootKmsKeyProvider);
}

}   // namespace NCloud::NBlockStore::NStorage
