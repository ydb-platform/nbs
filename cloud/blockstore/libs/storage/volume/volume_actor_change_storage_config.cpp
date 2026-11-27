#include "volume_actor.h"

#include "volume_database.h"

#include <cloud/blockstore/libs/storage/core/block_digest_factory.h>
#include <cloud/storage/core/config/markers.pb.h>
#include <cloud/storage/core/libs/common/proto_helpers.h>

#include <google/protobuf/util/message_differencer.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

namespace {

// Compare only the fields that an immutable RO view must retain.
bool ReadOnlySettingsChanged(const NProto::TStorageServiceConfig& previous,
                             const NProto::TStorageServiceConfig& next)
{
    google::protobuf::util::MessageDifferencer diff;
    const auto* descriptor = previous.GetDescriptor();
    for (int i = 0; i < descriptor->field_count(); ++i) {
        const auto* field = descriptor->field(i);
        if (field->options().GetExtension(NMarkers::AllowRuntimeUpdate)) {
            diff.IgnoreField(field);
        }
    }
    return !diff.Compare(previous, next);
}

// Preserve the administrative restart for settings captured during partition
// construction. These two policies are read for every I/O or mount request.
bool NeedsPartitionRestart(const NProto::TStorageServiceConfig& previous,
                           const NProto::TStorageServiceConfig& next)
{
    google::protobuf::util::MessageDifferencer diff;
    const auto* descriptor = previous.GetDescriptor();
    for (const auto number:
         {NProto::TStorageServiceConfig::kMaxReadWriteRangeSizeFieldNumber,
          NProto::TStorageServiceConfig::
              kSendErrorOnAddClientConflictFieldNumber})
    {
        diff.IgnoreField(descriptor->FindFieldByNumber(number));
    }
    return !diff.Compare(previous, next);
}

}   // namespace

void TVolumeActor::ApplyStorageConfig(bool localPatchChanged)
{
    // Keep explicit fields even when their values match the global base.
    auto raw = GlobalStorageConfig;
    HasStorageConfigPatch = HasKnownFields(StorageConfigPatch);
    if (HasStorageConfigPatch) {
        auto merged = std::make_shared<NProto::TStorageServiceConfig>(*raw);
        merged->MergeFrom(StorageConfigPatch);
        raw = std::move(merged);
    }

    // Administrative RO changes require a new immutable view. Ordinary RW
    // updates reach existing readers through the volume's original object.
    if (localPatchChanged &&
        ReadOnlySettingsChanged(*Config->GetConfigProto(), *raw))
    {
        Config = Config->ReplaceReadOnlyConfig(std::move(raw));
    } else {
        Config->ApplyRwConfig(std::move(raw));
    }
}

void TVolumeActor::HandleStorageConfigChanged(
    const TEvConfigsManager::TEvConfigChanged::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);
    const auto& published = ev->Get()->Config->GetStorageServiceConfig();
    if (GlobalStorageConfig == published) {
        return;
    }
    GlobalStorageConfig = published;
    ApplyStorageConfig(/*localPatchChanged=*/false);
}

bool TVolumeActor::PrepareChangeStorageConfig(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TChangeStorageConfig& args)
{
    Y_UNUSED(ctx);

    TVolumeDatabase db(tx.DB);
    if (args.MergeWithStorageConfigFromVolumeDB) {
        return db.ReadStorageConfig(args.StorageConfigFromDB);
    }
    return true;
}

void TVolumeActor::ExecuteChangeStorageConfig(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxVolume::TChangeStorageConfig& args)
{
    Y_UNUSED(ctx);

    TVolumeDatabase db(tx.DB);
    if (args.StorageConfigFromDB.Defined()) {
        args.ResultStorageConfig = std::move(*args.StorageConfigFromDB);
    }
    args.ResultStorageConfig.MergeFrom(args.StorageConfigNew);
    db.WriteStorageConfig(args.ResultStorageConfig);
}

void TVolumeActor::CompleteChangeStorageConfig(
    const TActorContext& ctx,
    TTxVolume::TChangeStorageConfig& args)
{
    auto response =
        std::make_unique<TEvVolume::TEvChangeStorageConfigResponse>();
    *response->Record.MutableStorageConfig() = args.ResultStorageConfig;

    LWTRACK(
        ResponseSent_Volume,
        args.RequestInfo->CallContext->LWOrbit,
        "ChangeStorageConfig",
        args.RequestInfo->CallContext->RequestId);

    // Apply the committed patch against the most recently processed
    // publication.
    const auto previous = Config->GetConfigProto();
    StorageConfigPatch = std::move(args.ResultStorageConfig);
    ApplyStorageConfig(/*localPatchChanged=*/true);

    // Preserve the existing lifecycle for changes to initialized child state.
    if (NeedsPartitionRestart(*previous, *Config->GetConfigProto())) {
        State->SetStorageConfig(Config);
        BlockDigestGenerator =
            BlockDigestGeneratorFactory->CreateBlockDigestGenerator(*Config);
        if (State->GetPartitionsState() == TPartitionInfo::READY ||
            State->GetPartitionsState() == TPartitionInfo::STARTED)
        {
            StopPartitions(ctx, {});
            StartPartitionsForUse(ctx);
        }
    }

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleChangeStorageConfig(
    const TEvVolume::TEvChangeStorageConfigRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        MakeIntrusive<TCallContext>());

    const auto* msg = ev->Get();
    ExecuteTx(ctx, CreateTx<TChangeStorageConfig>(
        requestInfo,
        msg->Record.GetStorageConfig(),
        msg->Record.GetMergeWithStorageConfigFromVolumeDB()));
}

}   // namespace NCloud::NBlockStore::NStorage
