#include "service_actor.h"

#include <cloud/filestore/libs/storage/core/helpers.h>
#include <cloud/filestore/libs/storage/core/model.h>

#include <ydb/core/protos/filestore_config.pb.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleDescribeFileStoreModel(
    const TEvService::TEvDescribeFileStoreModelRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto& request = ev->Get()->Record;

    if (request.GetBlockSize() == 0 || request.GetBlocksCount() == 0) {
        auto response = std::make_unique<TEvService::TEvDescribeFileStoreModelResponse>(
            MakeError(E_ARGUMENT, "zero block count or blocks size"));

        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    NKikimrFileStore::TConfig config;
    config.SetBlockSize(request.GetBlockSize());
    config.SetBlocksCount(request.GetBlocksCount());
    config.SetStorageMediaKind(request.GetStorageMediaKind());

    SetupFileStorePerformanceAndChannels(
        false,  // do not allocate mixed0 channel
        *StorageConfig,
        config,
        {}      // clientPerformanceProfile
    );

    auto response = std::make_unique<TEvService::TEvDescribeFileStoreModelResponse>();
    auto* model = response->Record.MutableFileStoreModel();
    model->SetBlockSize(request.GetBlockSize());
    model->SetBlocksCount(request.GetBlocksCount());
    model->SetStorageMediaKind(request.GetStorageMediaKind());
    model->SetChannelsCount(config.ExplicitChannelProfilesSize());

    Convert(config, *model->MutablePerformanceProfile());

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
