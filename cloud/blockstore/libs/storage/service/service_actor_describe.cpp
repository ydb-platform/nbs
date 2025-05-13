#include "service_actor.h"

#include "contrib/ydb/core/tx/scheme_cache/scheme_cache.h"

#include <cloud/blockstore/libs/storage/api/disk_registry.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <cloud/storage/core/libs/common/media.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDescribeVolumeActor final
    : public TActorBootstrapped<TDescribeVolumeActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TStorageConfigPtr Config;
    const TString DiskId;

    NProto::TVolume Volume;

public:
    TDescribeVolumeActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TString diskId);

    void Bootstrap(const TActorContext& ctx);

private:
    void DescribeVolume(const TActorContext& ctx);
    void DescribeVolumeCashe(const TActorContext& ctx);
    void DescribeDiskRegistryVolume(const TActorContext& ctx);

    void HandleDescribeVolumeResponse(
        const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDescribeDiskResponse(
        const TEvDiskRegistry::TEvDescribeDiskResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvService::TEvDescribeVolumeResponse> response);

    void HandleDescribeVolumeFromSchemeCacheResponse(
        const TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev,
        const TActorContext& ctx);

        private: STFUNC(StateDescribeVolume);
};

////////////////////////////////////////////////////////////////////////////////

TDescribeVolumeActor::TDescribeVolumeActor(
    TRequestInfoPtr requestInfo,
    TStorageConfigPtr config,
    TString diskId)
    : RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , DiskId(std::move(diskId))
{}

void TDescribeVolumeActor::Bootstrap(const TActorContext& ctx)
{
    DescribeVolumeCashe(ctx);
}

void TDescribeVolumeActor::DescribeVolume(const TActorContext& ctx)
{
    Become(&TThis::StateDescribeVolume);

    auto request =
        std::make_unique<TEvSSProxy::TEvDescribeVolumeRequest>(DiskId);

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TDescribeVolumeActor::DescribeVolumeCashe(const TActorContext& ctx)
{
    Become(&TThis::StateDescribeVolume);
    Cerr<<"Started !!!!"<<Endl;

    auto request = TAutoPtr<NSchemeCache::TSchemeCacheNavigate>(
        new NSchemeCache::TSchemeCacheNavigate());
    auto& entry = request->ResultSet.emplace_back();

    entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpPath;
    entry.SyncVersion = true;
    entry.RequestType = NSchemeCache::TSchemeCacheNavigate::TEntry::ERequestType::ByPath;
    entry.Path = {DiskId};

    auto event = std::make_unique<TEvTxProxySchemeCache::TEvNavigateKeySet>(std::move(request));
    NCloud::Send(
        ctx,
        MakeSchemeCacheID(),
        std::move(event),
        RequestInfo->Cookie);
}

void TDescribeVolumeActor::DescribeDiskRegistryVolume(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvDiskRegistry::TEvDescribeDiskRequest>();
    request->Record.SetDiskId(DiskId);

    NCloud::Send(
        ctx,
        MakeDiskRegistryProxyServiceId(),
        std::move(request),
        RequestInfo->Cookie);
}

void TDescribeVolumeActor::HandleDescribeVolumeResponse(
    const TEvSSProxy::TEvDescribeVolumeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Volume %s: describe failed: %s",
            DiskId.Quote().data(),
            FormatError(error).data());

        ReplyAndDie(
            ctx,
            std::make_unique<TEvService::TEvDescribeVolumeResponse>(error));
        return;
    }

    const auto& pathDescription = msg->PathDescription;
    const auto& volumeDescription =
        pathDescription.GetBlockStoreVolumeDescription();
    const auto& volumeConfig = volumeDescription.GetVolumeConfig();

    VolumeConfigToVolume(volumeConfig, Volume);
    Volume.SetTokenVersion(volumeDescription.GetTokenVersion());

    if (IsDiskRegistryMediaKind(Volume.GetStorageMediaKind())) {
        DescribeDiskRegistryVolume(ctx);
        return;
    }

    auto response = std::make_unique<TEvService::TEvDescribeVolumeResponse>();
    *response->Record.MutableVolume() = std::move(Volume);

    ReplyAndDie(ctx, std::move(response));
}

void TDescribeVolumeActor::HandleDescribeDiskResponse(
    const TEvDiskRegistry::TEvDescribeDiskResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    const auto& error = msg->GetError();
    if (FAILED(error.GetCode())) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "Non-replicated volume %s: describe failed: %s",
            DiskId.Quote().data(),
            FormatError(error).data());

        ReplyAndDie(
            ctx,
            std::make_unique<TEvService::TEvDescribeVolumeResponse>(error));
        return;
    }

    FillDeviceInfo(
        msg->Record.GetDevices(),
        msg->Record.GetMigrations(),
        msg->Record.GetReplicas(),
        msg->Record.GetDeviceReplacementUUIDs(),
        Volume);

    auto response = std::make_unique<TEvService::TEvDescribeVolumeResponse>();
    *response->Record.MutableVolume() = std::move(Volume);

    ReplyAndDie(ctx, std::move(response));
}

void TDescribeVolumeActor::HandleDescribeVolumeFromSchemeCacheResponse(
    const TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& resultSet = msg->Request->ResultSet;
    Cerr<<"handling !!!!"<<Endl;

    if (resultSet.empty()) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "Volume %s: scheme cache returned empty result set",
            DiskId.Quote().data());

        ReplyAndDie(ctx, std::make_unique<TEvService::TEvDescribeVolumeResponse>(
            MakeError(E_FAIL, "Empty result set from scheme cache")));
        return;
    }
    Cerr<<"Not empty !!!!"<<Endl;

    const auto& entry = resultSet.front();

    if (entry.Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "Volume %s: scheme cache resolution failed: status=%s",
            DiskId.Quote().data(),
            entry.Status);

        ReplyAndDie(ctx, std::make_unique<TEvService::TEvDescribeVolumeResponse>(
            MakeError(E_FAIL, TStringBuilder() << "Resolution failed: "
                                               << entry.Status)));
        return;
    }
    Cerr<<"NSchemeCache::TSchemeCacheNavigate::EStatus::Ok !!!!"<<Endl;
    entry

    const auto it = entry.Attributes.find("BlockStoreVolumeDescription");
    if (it == entry.Attributes.end()) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "Volume %s: BlockStoreVolumeDescription attribute not found",
            DiskId.Quote().data());

        ReplyAndDie(ctx, std::make_unique<TEvService::TEvDescribeVolumeResponse>(
            MakeError(E_FAIL, "Missing BlockStoreVolumeDescription attribute")));
        return;
    }
    Cerr<<"NO BlockStoreVolumeDescription !!!!"<<Endl;

    NKikimrSchemeOp::TBlockStoreVolumeDescription volumeDescription;
    if (!volumeDescription.ParseFromString(it->second)) {
        LOG_ERROR(ctx, TBlockStoreComponents::SERVICE,
            "Volume %s: failed to parse BlockStoreVolumeDescription",
            DiskId.Quote().data());

        ReplyAndDie(ctx, std::make_unique<TEvService::TEvDescribeVolumeResponse>(
            MakeError(E_FAIL, "Failed to parse volume description")));
        return;
    }
    Cerr<<"NO it->second !!!!"<<Endl;

    const auto& volumeConfig = volumeDescription.GetVolumeConfig();

    VolumeConfigToVolume(volumeConfig, Volume);
    Volume.SetTokenVersion(volumeDescription.GetTokenVersion());

    if (IsDiskRegistryMediaKind(Volume.GetStorageMediaKind())) {
        DescribeDiskRegistryVolume(ctx);
        return;
    }

    auto response = std::make_unique<TEvService::TEvDescribeVolumeResponse>();
    *response->Record.MutableVolume() = std::move(Volume);
    Cerr<<"ALl FINE!!!!"<<Endl;

    ReplyAndDie(ctx, std::move(response));
}

void TDescribeVolumeActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvService::TEvDescribeVolumeResponse> response)
{
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDescribeVolumeActor::StateDescribeVolume)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(
            TEvSSProxy::TEvDescribeVolumeResponse,
            HandleDescribeVolumeResponse);
        HFunc(
            TEvDiskRegistry::TEvDescribeDiskResponse,
            HandleDescribeDiskResponse);
        HFunc(
            TEvTxProxySchemeCache::TEvNavigateKeySetResult,
            HandleDescribeVolumeFromSchemeCacheResponse);
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

void TServiceActor::HandleDescribeVolume(
    const TEvService::TEvDescribeVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    const auto& request = msg->Record;

    if (request.GetDiskId().empty()) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::SERVICE,
            "Empty DiskId in DescribeVolume");

        auto response = std::make_unique<TEvService::TEvDescribeVolumeResponse>(
            MakeError(E_ARGUMENT, "Volume name cannot be empty"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::SERVICE,
        "Describing volume: %s",
        request.GetDiskId().Quote().data());

    NCloud::Register<TDescribeVolumeActor>(
        ctx,
        std::move(requestInfo),
        Config,
        request.GetDiskId());
}

}   // namespace NCloud::NBlockStore::NStorage
