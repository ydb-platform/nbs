#include "cloud/filestore/libs/storage/api/ss_proxy.h"

#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/tablet/tablet_private.h>
#include <cloud/filestore/libs/storage/testlib/service_client.h>

#include <contrib/ydb/library/actors/interconnect/types.h>

#include <util/generic/fwd.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

struct TShardedFileSystemConfig
{
    const TString FsId = "test";
    const TString Shard1Id = FsId + "_s1";
    const TString Shard2Id = FsId + "_s2";
    const ui64 ShardBlockCount = 1'000;

    bool DirectoryCreationInShardsEnabled = false;

    TVector<TString> ShardIds() const
    {
        return {Shard1Id, Shard2Id};
    }

    TVector<TString> MainAndShardIds() const
    {
        return {FsId, Shard1Id, Shard2Id};
    }
};

struct TFileSystemInfo
{
    ui64 MainTabletId = -1;
    ui64 Shard1TabletId = -1;
    ui64 Shard2TabletId = -1;
    TActorId MainTabletActorId;
    TActorId Shard1ActorId;
    TActorId Shard2ActorId;
};

static TFileSystemInfo CreateFileSystem(
    TServiceClient& service,
    const TShardedFileSystemConfig& fsConfig)
{
    TFileSystemInfo info;
    bool configureShardsRequestObserved = false;
    auto prevFilter = service.AccessRuntime().SetEventFilter(
        [&](auto& runtime, TAutoPtr<IEventHandle>& event)
        {
            Y_UNUSED(runtime);
            switch (event->GetTypeRewrite()) {
                case TEvSSProxy::EvDescribeFileStoreResponse: {
                    using TDesc = TEvSSProxy::TEvDescribeFileStoreResponse;
                    const auto* msg = event->Get<TDesc>();
                    const auto& desc =
                        msg->PathDescription.GetFileStoreDescription();
                    if (desc.GetConfig().GetFileSystemId() == fsConfig.FsId) {
                        info.MainTabletId = desc.GetIndexTabletId();
                    }
                    if (desc.GetConfig().GetFileSystemId() == fsConfig.Shard1Id)
                    {
                        info.Shard1TabletId = desc.GetIndexTabletId();
                    }
                    if (desc.GetConfig().GetFileSystemId() == fsConfig.Shard2Id)
                    {
                        info.Shard2TabletId = desc.GetIndexTabletId();
                    }

                    break;
                }

                case TEvIndexTablet::EvConfigureAsShardRequest: {
                    using R = TEvIndexTablet::TEvConfigureAsShardRequest;
                    const auto* msg = event->Get<R>();
                    if (fsConfig.Shard1Id == msg->Record.GetFileSystemId()) {
                        info.Shard1ActorId = event->Recipient;
                    }
                    if (fsConfig.Shard2Id == msg->Record.GetFileSystemId()) {
                        info.Shard2ActorId = event->Recipient;
                    }
                    break;
                }

                case TEvIndexTablet::EvConfigureShardsRequest: {
                    configureShardsRequestObserved = true;
                    break;
                }

                case TEvIndexTabletPrivate::EvLoadCompactionMapChunkRequest: {
                    // The first tablet to start after ConfigureShards
                    // request is sent is the main tablet (after suiciding)
                    if (configureShardsRequestObserved) {
                        info.MainTabletActorId = event->Recipient;
                    }
                    break;
                }
            }

            return false;
        });

    service.CreateFileStore(fsConfig.FsId, fsConfig.ShardBlockCount * 2);

    service.AccessRuntime().DispatchEvents(
        {.CustomFinalCondition = [&]() -> bool
         {
             return static_cast<bool>(info.MainTabletActorId);
         }});

    service.AccessRuntime().SetEventFilter(prevFilter);

    UNIT_ASSERT_VALUES_UNEQUAL(-1, info.MainTabletId);
    UNIT_ASSERT_VALUES_UNEQUAL(-1, info.Shard1TabletId);
    UNIT_ASSERT_VALUES_UNEQUAL(-1, info.Shard2TabletId);
    UNIT_ASSERT(info.MainTabletActorId);
    UNIT_ASSERT(info.Shard1ActorId);
    UNIT_ASSERT(info.Shard2ActorId);

    return info;
}

}   // namespace NCloud::NFileStore::NStorage
