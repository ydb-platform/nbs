#include "service_actor.h"

#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/volume_model.h>

#include <ydb/core/protos/blockstore_config.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleDescribeVolumeModel(
    const TEvService::TEvDescribeVolumeModelRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& request = msg->Record;

    LOG_INFO(ctx, TBlockStoreComponents::SERVICE, "Describing volume model");

    auto partitionsInfo = ComputePartitionsInfo(
        *Config,
        {}, // cloudId
        {}, // folderId
        {}, // diskId
        request.GetStorageMediaKind(),
        request.GetBlocksCount(),
        request.GetBlockSize(),
        request.GetIsSystem(),
        !request.GetBaseDiskId().empty()
    );

    TVolumeParams volumeParams;
    volumeParams.BlockSize = request.GetBlockSize();
    volumeParams.PartitionsCount = partitionsInfo.PartitionsCount;
    volumeParams.BlocksCountPerPartition =
        partitionsInfo.BlocksCountPerPartition;
    volumeParams.MediaKind = request.GetStorageMediaKind();

    NKikimrBlockStore::TVolumeConfig config;
    config.SetBlockSize(request.GetBlockSize());
    ResizeVolume(*Config, volumeParams, {}, {}, config);

    auto response = std::make_unique<TEvService::TEvDescribeVolumeModelResponse>();
    VolumeConfigToVolumeModel(config, *response->Record.MutableVolumeModel());

    NCloud::Send(ctx, ev->Sender, std::move(response), ev->Cookie);
}

}   // namespace NCloud::NBlockStore::NStorage
