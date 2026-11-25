#include "service_ut_helpers.h"

#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/disk_registry_proxy/api/service.h>
#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>
#include <cloud/filestore/libs/storage/testlib/service_client.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>
#include <cloud/filestore/libs/storage/testlib/test_fast_shard.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/media.pb.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

#include <google/protobuf/util/message_differencer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash_set.h>
#include <util/generic/map.h>
#include <util/generic/size_literals.h>
#include <util/string/builder.h>
#include <util/string/join.h>

#include <memory>
#include <utility>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 AllocationSize = 1_GB + 4_MB;
constexpr ui64 BlocksCount = 262'144;

NProto::TStorageConfig MakeFastShardConfig()
{
    auto config = MakeStorageConfig();
    config.SetFastShardRuntimeEnabled(true);
    config.SetDirectoryCreationInShardsEnabled(true);
    config.SetFastShardAllocationSize(AllocationSize);
    config.SetFastShardReservedSize(4_MB);
    config.SetFastShardDevicePoolName("pool");
    config.SetSizeToNodesRatio(1_MB);
    return config;
}

////////////////////////////////////////////////////////////////////////////////

struct TTestDiskRegistry
{
    TMap<TString, TEvDiskRegistryProxy::TAllocateDevicesRequest> Disks;
    THashSet<TString> MarkedForCleanup;
    TString Host = "host";
};

class TTestDiskRegistryProxy final: public TActor<TTestDiskRegistryProxy>
{
private:
    const std::shared_ptr<TTestDiskRegistry> DiskRegistry;

public:
    explicit TTestDiskRegistryProxy(
            std::shared_ptr<TTestDiskRegistry> diskRegistry)
        : TActor(&TThis::StateWork)
        , DiskRegistry(std::move(diskRegistry))
    {}

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(
                TEvDiskRegistryProxy::TEvAllocateDevicesRequest,
                HandleAllocateDevices);
            HFunc(
                TEvDiskRegistryProxy::TEvMarkForCleanupRequest,
                HandleMarkForCleanup);
            HFunc(
                TEvDiskRegistryProxy::TEvDeallocateDevicesRequest,
                HandleDeallocateDevices);
        }
    }

    NProto::TError CheckOwner(const TString& diskId, ui64 tabletId) const
    {
        const auto* disk = DiskRegistry->Disks.FindPtr(diskId);
        if (!disk) {
            return MakeError(E_NOT_FOUND);
        }
        if (disk->TabletId != tabletId) {
            return MakeError(E_ARGUMENT);
        }
        return {};
    }

    void HandleAllocateDevices(
        const TEvDiskRegistryProxy::TEvAllocateDevicesRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        // A repeated request gets the same devices.
        const auto& disk = DiskRegistry->Disks
            .try_emplace(msg->FileSystemId, *msg)
            .first->second;

        TEvDiskRegistryProxy::TDeviceLayout layout;
        for (ui32 i = 0; i <= disk.ReplicaCount; ++i) {
            auto& device = layout.Replicas.emplace_back().emplace_back();
            device.SetHost(DiskRegistry->Host);
            device.SetDeviceId(
                TStringBuilder() << msg->FileSystemId << "-" << i);
        }

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskRegistryProxy::TEvAllocateDevicesResponse>(
                std::move(layout)));
    }

    void HandleMarkForCleanup(
        const TEvDiskRegistryProxy::TEvMarkForCleanupRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        const auto error = CheckOwner(msg->FileSystemId, msg->TabletId);
        if (!HasError(error)) {
            DiskRegistry->MarkedForCleanup.insert(msg->FileSystemId);
        }

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskRegistryProxy::TEvMarkForCleanupResponse>(
                error));
    }

    void HandleDeallocateDevices(
        const TEvDiskRegistryProxy::TEvDeallocateDevicesRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        auto error = CheckOwner(msg->FileSystemId, msg->TabletId);
        // Only a disk that is marked for cleanup is deallocated.
        if (!HasError(error) &&
            !DiskRegistry->MarkedForCleanup.erase(msg->FileSystemId))
        {
            error = MakeError(E_INVALID_STATE);
        }
        if (!HasError(error)) {
            DiskRegistry->Disks.erase(msg->FileSystemId);
        }

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskRegistryProxy::TEvDeallocateDevicesResponse>(
                error));
    }
};

////////////////////////////////////////////////////////////////////////////////

// The shards start at once. The config of a shard is the one of its last start.
struct TTestFastShardFactory final: NFastShard::IFileSystemShardFactory
{
    TMap<TString, NProtoPrivate::TPersistentFastShardConfig> Configs;

    NFastShard::IFileSystemShardPtr CreateShard(
        const TString& fileSystemId,
        const NProtoPrivate::TFastShardConfig& config,
        ui32 shardNo,
        ui64 generation) override
    {
        Y_UNUSED(shardNo, generation);

        Configs[fileSystemId] = config.GetPersistentConfig();
        auto shard = std::make_shared<TTestFastShard>();
        shard->InitResult.SetValue({});
        return shard;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFastShardTestEnv
{
    const std::shared_ptr<TTestFastShardFactory> Shards =
        std::make_shared<TTestFastShardFactory>();
    const std::shared_ptr<TTestDiskRegistry> DiskRegistry =
        std::make_shared<TTestDiskRegistry>();
    TTestEnv Env;
    const ui32 NodeIdx;
    TServiceClient Service;

    explicit TFastShardTestEnv(const NProto::TStorageConfig& config)
        : Env({.FastShardFactory = Shards}, config)
        , NodeIdx(Env.AddDynamicNode())
        , Service(Env.GetRuntime(), NodeIdx)
    {
        auto& runtime = Env.GetRuntime();
        const auto proxyId =
            runtime.Register(new TTestDiskRegistryProxy(DiskRegistry), NodeIdx);
        runtime.RegisterService(
            MakeFileStoreDiskRegistryProxyId(),
            proxyId,
            NodeIdx);
    }
};

template <typename TRequest, typename TResponse>
TTestActorRuntimeBase::TEventObserverHolder FailRequest(
    TFastShardTestEnv& env,
    TString fileSystemId)
{
    return env.Env.GetRuntime().AddObserver(
        [=, failed = false] (TAutoPtr<IEventHandle>& event) mutable {
            if (!failed && event->GetTypeRewrite() == TRequest::EventType &&
                event->template Get<TRequest>()->FileSystemId == fileSystemId)
            {
                failed = true;
                event.Reset(new IEventHandle(
                    event->Sender,
                    event->Recipient,
                    new TResponse(MakeError(E_REJECTED)),
                    0,   // flags
                    event->Cookie));
            }
        });
}

auto CreateFileStore(
    TFastShardTestEnv& env,
    const TString& id,
    ui64 blocksCount,
    ui32 shardCount,
    ui32 fastShardCount,
    NProto::EFastShardStorageKind kind)
{
    auto request = env.Service.CreateCreateFileStoreRequest(id, blocksCount);
    request->Record.SetShardCount(shardCount);
    request->Record.SetFastShardCount(fastShardCount);
    request->Record.SetFastShardStorageKind(kind);
    env.Service.SendCreateFileStoreRequest(std::move(request));
    return env.Service.RecvCreateFileStoreResponse()->GetStatus();
}

auto ResizeFileStore(
    TFastShardTestEnv& env,
    const TString& id,
    ui64 blocksCount,
    ui32 shardCount,
    ui32 fastShardCount)
{
    auto request = env.Service.CreateResizeFileStoreRequest(
        id,
        blocksCount,
        false /* force */,
        shardCount);
    request->Record.SetFastShardCount(fastShardCount);
    env.Service.SendResizeFileStoreRequest(std::move(request));
    return env.Service.RecvResizeFileStoreResponse()->GetStatus();
}

auto DestroyFileStore(TFastShardTestEnv& env, const TString& id)
{
    env.Service.SendDestroyFileStoreRequest(id);
    return env.Service.RecvDestroyFileStoreResponse()->GetStatus();
}

void RestartService(
    TFastShardTestEnv& env,
    const NProto::TStorageConfig& config)
{
    env.Env.UpdateStorageConfig(config);
    env.Env.CreateAndRegisterStorageService(env.NodeIdx);
}

TString GetDiskIds(const TFastShardTestEnv& env)
{
    TVector<TString> ids;
    for (const auto& [id, disk]: env.DiskRegistry->Disks) {
        ids.push_back(id);
    }
    return JoinSeq(",", ids);
}

TString GetFileStoreIds(TFastShardTestEnv& env)
{
    const auto response = env.Service.ListFileStores();
    TVector<TString> ids(
        response->Record.GetFileStores().begin(),
        response->Record.GetFileStores().end());
    Sort(ids);
    return JoinSeq(",", ids);
}

TString GetFastShardIds(TFastShardTestEnv& env, const TString& id)
{
    const auto topology = GetFileSystemTopology(env.Service, id);
    TVector<TString> ids;
    for (const auto& shard: topology.GetShardTopology().GetShards()) {
        if (shard.GetType() == NProtoPrivate::TShardTopology::E_FASTSHARD) {
            ids.push_back(shard.GetShardId());
        }
    }
    return JoinSeq(",", ids);
}

// Waits for the start of the fast shard.
NProtoPrivate::TPersistentFastShardConfig GetFastShardConfig(
    TFastShardTestEnv& env,
    const TString& id)
{
    env.Env.GetRuntime().DispatchEvents({.CustomFinalCondition = [&] {
        return env.Shards->Configs.contains(id);
    }});
    return env.Shards->Configs.at(id);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStorageServiceFastShardTest)
{
    Y_UNIT_TEST(ShouldRejectInvalidFastShardRequests)
    {
        auto config = MakeFastShardConfig();
        TFastShardTestEnv env(config);

        const auto invalidKind = static_cast<NProto::EFastShardStorageKind>(2);
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            CreateFileStore(env, "test", BlocksCount, 2, 2, invalidKind));
        // A fast shard count without a storage kind should be rejected.
        {
            auto request =
                env.Service.CreateCreateFileStoreRequest("test", BlocksCount);
            request->Record.SetShardCount(2);
            request->Record.SetFastShardCount(2);
            env.Service.SendCreateFileStoreRequest(std::move(request));
            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                env.Service.RecvCreateFileStoreResponse()->GetStatus());
        }
        // The directories need a shard of their own.
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            CreateFileStore(
                env,
                "test",
                BlocksCount,
                0,
                2,
                NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            CreateFileStore(
                env,
                "test",
                BlocksCount,
                253,
                2,
                NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR));
        UNIT_ASSERT_VALUES_EQUAL("", GetFileStoreIds(env));

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            CreateFileStore(
                env,
                "test",
                BlocksCount,
                2,
                2,
                NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR));
        env.Service.CreateFileStore("plain", BlocksCount);

        // The shards cannot be dropped.
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            ResizeFileStore(env, "test", BlocksCount, 2, 1));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            ResizeFileStore(env, "test", BlocksCount, 1, 2));
        // The shards of both kinds count against the shard limit.
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            ResizeFileStore(env, "test", BlocksCount, 251, 4));
        // Fast shards are created with the filesystem only.
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            ResizeFileStore(env, "plain", BlocksCount, 2, 1));
        // A fast shard is not resized on its own.
        const ui64 shardBlocksCount = env.Service.GetFileStoreInfo("test_s3")
            ->Record.GetFileStore()
            .GetBlocksCount();
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            ResizeFileStore(env, "test_s3", 2 * shardBlocksCount, 0, 0));

        // The directories of such a filesystem are created in the other shards.
        config.SetDirectoryCreationInShardsEnabled(false);
        RestartService(env, config);
        UNIT_ASSERT_VALUES_EQUAL(
            E_PRECONDITION_FAILED,
            CreateFileStore(
                env,
                "test2",
                BlocksCount / 2,
                2,
                2,
                NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR));

        // A rejected request changes nothing.
        UNIT_ASSERT_VALUES_EQUAL(
            "plain,test,test_s1,test_s2,test_s3,test_s4",
            GetFileStoreIds(env));
        UNIT_ASSERT_VALUES_EQUAL(
            "test_s3,test_s4",
            GetFastShardIds(env, "test"));
        UNIT_ASSERT_VALUES_EQUAL("test_s3,test_s4", GetDiskIds(env));
    }

    Y_UNIT_TEST(ShouldNotAddShardsToShard)
    {
        TFastShardTestEnv env(MakeFastShardConfig());

        // A filesystem with two ordinary and two fast shards.
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            CreateFileStore(
                env,
                "test",
                BlocksCount,
                2,
                2,
                NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR));

        const ui64 shardBlocksCount = env.Service.GetFileStoreInfo("test_s1")
            ->Record.GetFileStore()
            .GetBlocksCount();

        // A resize of a shard that asks for shards should be rejected.
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            ResizeFileStore(env, "test_s1", 2 * shardBlocksCount, 2, 0));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            ResizeFileStore(env, "test_s1", 2 * shardBlocksCount, 0, 1));

        // A rejected request changes nothing.
        UNIT_ASSERT_VALUES_EQUAL(
            "test,test_s1,test_s2,test_s3,test_s4",
            GetFileStoreIds(env));
        UNIT_ASSERT_VALUES_EQUAL(
            shardBlocksCount,
            env.Service.GetFileStoreInfo("test_s1")
                ->Record.GetFileStore()
                .GetBlocksCount());
    }

    Y_UNIT_TEST(ShouldNotAddFastShardsIfShardCountIsCapped)
    {
        auto config = MakeFastShardConfig();
        config.SetMaxShardCount(1024);
        config.SetOneByteShardCountCapEnabled(true);
        TFastShardTestEnv env(config);

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            CreateFileStore(
                env,
                "test",
                BlocksCount,
                2,
                2,
                NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR));

        // A resize over 255 shards that adds a fast shard should be rejected.
        UNIT_ASSERT_VALUES_EQUAL(
            E_PRECONDITION_FAILED,
            ResizeFileStore(env, "test", BlocksCount, 254, 3));
        UNIT_ASSERT_VALUES_EQUAL(
            "test,test_s1,test_s2,test_s3,test_s4",
            GetFileStoreIds(env));
        UNIT_ASSERT_VALUES_EQUAL("test_s3,test_s4", GetDiskIds(env));
    }

    Y_UNIT_TEST(ShouldAllocateDevicesForFastShards)
    {
        TFastShardTestEnv env(MakeFastShardConfig());

        struct TCase
        {
            TString Id;
            ui64 Size;
            NProto::EFastShardStorageKind Kind;
            NProto::EStorageMediaKind MediaKind;
            ui32 ReplicaCount;
        };

        // The replica count leaves out the main copy.
        const TCase cases[] = {
            {
                "mirror",
                512_MB,
                NProto::FAST_SHARD_STORAGE_MIRROR,
                NProto::STORAGE_MEDIA_SSD_MIRROR2,
                1,
            },
            {
                "quorum",
                1_GB,
                NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR,
                NProto::STORAGE_MEDIA_SSD_MIRROR3,
                2,
            },
        };

        for (const auto& c: cases) {
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                CreateFileStore(env, c.Id, c.Size / 4_KB, 1, 2, c.Kind));
            UNIT_ASSERT_VALUES_EQUAL(
                c.Id + "_s2," + c.Id + "_s3",
                GetFastShardIds(env, c.Id));

            for (const TString& shardId: {c.Id + "_s2", c.Id + "_s3"}) {
                // The tablet owns a disk of the configured size and pool.
                const auto& disk = env.DiskRegistry->Disks.at(shardId);
                UNIT_ASSERT_VALUES_EQUAL(
                    env.Service.GetFileStoreInfo(shardId)
                        ->Record.GetFileStore()
                        .GetMainTabletId(),
                    disk.TabletId);
                UNIT_ASSERT_EQUAL(c.MediaKind, disk.MediaKind);
                UNIT_ASSERT_VALUES_EQUAL(c.ReplicaCount, disk.ReplicaCount);
                UNIT_ASSERT_VALUES_EQUAL(
                    AllocationSize / 4_KB,
                    disk.DeviceBlocksCount);
                UNIT_ASSERT_VALUES_EQUAL("pool", disk.DevicePoolName);

                // The capacity is the allocation size minus the reserve.
                const auto config = GetFastShardConfig(env, shardId);
                UNIT_ASSERT_VALUES_EQUAL(
                    1_GB,
                    config.GetExpectedGroupCapacity());
                UNIT_ASSERT_VALUES_EQUAL(
                    1_GB / 1_MB,
                    config.GetNodesPerGroup());
                // The shard gets the devices of its disk.
                const auto& group = config.GetStorageGroups(0);
                UNIT_ASSERT_EQUAL(c.Kind, group.GetType());
                UNIT_ASSERT_VALUES_EQUAL(
                    c.ReplicaCount + 1,
                    group.DevicesSize());
                UNIT_ASSERT_VALUES_EQUAL(
                    TStringBuilder() << shardId << "-" << c.ReplicaCount,
                    group.GetDevices(c.ReplicaCount).GetDeviceId());
            }

            // The main tablet keeps the block size as the page size.
            const auto topology = GetFileSystemTopology(env.Service, c.Id);
            for (const auto& shard: topology.GetShardTopology().GetShards()) {
                if (shard.GetType() ==
                    NProtoPrivate::TShardTopology::E_FASTSHARD)
                {
                    UNIT_ASSERT_VALUES_EQUAL(
                        4_KB,
                        shard.GetFastShardConfig()
                            .GetPersistentConfig()
                            .GetPageSize());
                }
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(
            "mirror_s2,mirror_s3,quorum_s2,quorum_s3",
            GetDiskIds(env));

        // A fast shard cannot use a device without a host.
        env.DiskRegistry->Host = "";
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            CreateFileStore(
                env,
                "hostless",
                BlocksCount,
                1,
                2,
                NProto::FAST_SHARD_STORAGE_MIRROR));
    }

    Y_UNIT_TEST(ShouldAddFastShardsForFileStoreSize)
    {
        TFastShardTestEnv env(MakeFastShardConfig());

        // A fast shard holds 1 GiB, a filesystem of 3 GiB needs three of them.
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            CreateFileStore(
                env,
                "test",
                3 * BlocksCount,
                1,
                2,
                NProto::FAST_SHARD_STORAGE_MIRROR));
        // Without a fast shard count it gets them.
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            CreateFileStore(
                env,
                "test",
                3 * BlocksCount,
                1,
                0,
                NProto::FAST_SHARD_STORAGE_MIRROR));
        UNIT_ASSERT_VALUES_EQUAL(
            "test,test_s1,test_s2,test_s3,test_s4",
            GetFileStoreIds(env));
        UNIT_ASSERT_VALUES_EQUAL(
            "test_s2,test_s3,test_s4",
            GetFastShardIds(env, "test"));

        // A resize to the same size adds no shards.
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            ResizeFileStore(env, "test", 3 * BlocksCount, 0, 0));
        UNIT_ASSERT_VALUES_EQUAL(
            "test,test_s1,test_s2,test_s3,test_s4",
            GetFileStoreIds(env));

        // A bigger filesystem gets the fast shards for the added size.
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            ResizeFileStore(env, "test", 5 * BlocksCount, 0, 0));
        UNIT_ASSERT_VALUES_EQUAL(
            "test,test_s1,test_s2,test_s3,test_s4,test_s5,test_s6",
            GetFileStoreIds(env));
        UNIT_ASSERT_VALUES_EQUAL(
            "test_s2,test_s3,test_s4,test_s5,test_s6",
            GetFastShardIds(env, "test"));
        UNIT_ASSERT_VALUES_EQUAL(
            "test_s2,test_s3,test_s4,test_s5,test_s6",
            GetDiskIds(env));

        // A block over the capacity of the fast shards needs one more.
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            ResizeFileStore(env, "test", 5 * BlocksCount + 1, 0, 0));
        UNIT_ASSERT_VALUES_EQUAL(
            "test_s2,test_s3,test_s4,test_s5,test_s6,test_s7",
            GetFastShardIds(env, "test"));

        // An explicit fast shard count has to hold the filesystem too.
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            ResizeFileStore(env, "test", 7 * BlocksCount, 0, 6));
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            ResizeFileStore(env, "test", 7 * BlocksCount, 0, 7));
        UNIT_ASSERT_VALUES_EQUAL(
            "test_s2,test_s3,test_s4,test_s5,test_s6,test_s7,test_s8",
            GetFastShardIds(env, "test"));
    }

    Y_UNIT_TEST(ShouldResizeFileStoreWithFastShards)
    {
        auto storageConfig = MakeFastShardConfig();
        // The shards get their devices one at a time.
        storageConfig.SetMaxShardManagementRequestsInFlight(1);
        TFastShardTestEnv env(storageConfig);

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            CreateFileStore(
                env,
                "test",
                BlocksCount,
                2,
                2,
                NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR));
        const auto config = GetFastShardConfig(env, "test_s3");

        // A new fast shard gets the storage kind and the block size of the
        // filesystem.
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            ResizeFileStore(env, "test", BlocksCount, 3, 3));
        UNIT_ASSERT_VALUES_EQUAL(
            "test_s3,test_s4,test_s6",
            GetFastShardIds(env, "test"));
        UNIT_ASSERT_VALUES_EQUAL("test_s3,test_s4,test_s6", GetDiskIds(env));
        UNIT_ASSERT_EQUAL(
            NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR,
            GetFastShardConfig(env, "test_s6").GetStorageGroups(0).GetType());
        UNIT_ASSERT_VALUES_EQUAL(
            4_KB,
            GetFileSystemTopology(env.Service, "test")
                .GetShardTopology()
                .GetShards(5)
                .GetFastShardConfig()
                .GetPersistentConfig()
                .GetPageSize());

        // A resize without a fast shard count keeps the fast shards.
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            ResizeFileStore(env, "test", BlocksCount, 4, 0));
        UNIT_ASSERT_VALUES_EQUAL(
            "test_s3,test_s4,test_s6",
            GetFastShardIds(env, "test"));
        UNIT_ASSERT_VALUES_EQUAL("test_s3,test_s4,test_s6", GetDiskIds(env));

        // A resize without a shard count keeps the other shards.
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            ResizeFileStore(env, "test", BlocksCount, 0, 4));
        UNIT_ASSERT_VALUES_EQUAL(
            "test_s3,test_s4,test_s6,test_s8",
            GetFastShardIds(env, "test"));
        UNIT_ASSERT_VALUES_EQUAL(
            "test_s3,test_s4,test_s6,test_s8",
            GetDiskIds(env));
        UNIT_ASSERT_VALUES_EQUAL(
            "test,test_s1,test_s2,test_s3,test_s4,test_s5,test_s6,test_s7,test_s8",
            GetFileStoreIds(env));

        // The other shards create their files in the fast shards too.
        UNIT_ASSERT_VALUES_EQUAL(
            "test_s3,test_s4,test_s6,test_s8",
            GetFastShardIds(env, "test_s1"));
        UNIT_ASSERT_VALUES_EQUAL(
            "test_s3,test_s4,test_s6,test_s8",
            GetFastShardIds(env, "test_s7"));

        // A shard is resized apart from its filesystem.
        const ui64 shardBlocksCount = env.Service.GetFileStoreInfo("test_s1")
            ->Record.GetFileStore()
            .GetBlocksCount();
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            ResizeFileStore(env, "test_s1", 2 * shardBlocksCount, 0, 0));

        // A fast shard keeps its config over the resizes.
        env.Shards->Configs.erase("test_s3");
        TIndexTabletClient tablet(
            env.Env.GetRuntime(),
            env.NodeIdx,
            env.Service.GetFileStoreInfo("test_s3")
                ->Record.GetFileStore()
                .GetMainTabletId(),
            {},
            false /* updateConfig */);
        tablet.RebootTablet();
        UNIT_ASSERT(google::protobuf::util::MessageDifferencer::Equals(
            config,
            GetFastShardConfig(env, "test_s3")));
    }

    Y_UNIT_TEST(ShouldRetryCreateFileStoreWithFastShards)
    {
        TFastShardTestEnv env(MakeFastShardConfig());

        // A retry completes a create that failed in the allocation.
        {
            const auto failure = FailRequest<
                TEvDiskRegistryProxy::TEvAllocateDevicesRequest,
                TEvDiskRegistryProxy::TEvAllocateDevicesResponse>(
                env,
                "test_s3");
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                CreateFileStore(
                    env,
                    "test",
                    BlocksCount,
                    1,
                    2,
                    NProto::FAST_SHARD_STORAGE_MIRROR));
        }
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            CreateFileStore(
                env,
                "test",
                BlocksCount,
                1,
                2,
                NProto::FAST_SHARD_STORAGE_MIRROR));
        UNIT_ASSERT_VALUES_EQUAL(
            "test_s2,test_s3",
            GetFastShardIds(env, "test"));
        UNIT_ASSERT_VALUES_EQUAL("test_s2,test_s3", GetDiskIds(env));
    }

    Y_UNIT_TEST(ShouldRetryResizeFileStoreWithFastShards)
    {
        TFastShardTestEnv env(MakeFastShardConfig());

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            CreateFileStore(
                env,
                "test",
                BlocksCount,
                1,
                2,
                NProto::FAST_SHARD_STORAGE_MIRROR));

        // The filesystem does not use the new shards until the retry.
        {
            const auto failure = FailRequest<
                TEvDiskRegistryProxy::TEvAllocateDevicesRequest,
                TEvDiskRegistryProxy::TEvAllocateDevicesResponse>(
                env,
                "test_s5");
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                ResizeFileStore(env, "test", BlocksCount, 2, 3));
        }
        UNIT_ASSERT_VALUES_EQUAL(
            "test_s2,test_s3",
            GetFastShardIds(env, "test"));

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            ResizeFileStore(env, "test", BlocksCount, 2, 3));
        UNIT_ASSERT_VALUES_EQUAL(
            "test_s2,test_s3,test_s5",
            GetFastShardIds(env, "test"));
        UNIT_ASSERT_VALUES_EQUAL("test_s2,test_s3,test_s5", GetDiskIds(env));
    }

    Y_UNIT_TEST(ShouldDestroyFastShardWithoutItsSiblings)
    {
        TFastShardTestEnv env(MakeFastShardConfig());

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            CreateFileStore(
                env,
                "test",
                BlocksCount,
                1,
                2,
                NProto::FAST_SHARD_STORAGE_MIRROR));

        // A fast shard that is destroyed by its own id takes only its devices.
        // It is gone before its own destroy, which finds no shard.
        UNIT_ASSERT_VALUES_EQUAL(S_FALSE, DestroyFileStore(env, "test_s2"));
        UNIT_ASSERT_VALUES_EQUAL(
            "test,test_s1,test_s3",
            GetFileStoreIds(env));
        UNIT_ASSERT_VALUES_EQUAL("test_s3", GetDiskIds(env));
    }

    Y_UNIT_TEST(ShouldRetryDestroyFileStoreWithFastShards)
    {
        TFastShardTestEnv env(MakeFastShardConfig());

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            CreateFileStore(
                env,
                "test",
                BlocksCount,
                1,
                2,
                NProto::FAST_SHARD_STORAGE_MIRROR));

        // The devices are deallocated only after their shard is destroyed.
        {
            const auto failure = FailRequest<
                TEvSSProxy::TEvDestroyFileStoreRequest,
                TEvSSProxy::TEvDestroyFileStoreResponse>(env, "test_s2");
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, DestroyFileStore(env, "test"));
        }
        UNIT_ASSERT_VALUES_EQUAL(
            "test,test_s1,test_s2,test_s3",
            GetFileStoreIds(env));
        UNIT_ASSERT_VALUES_EQUAL("test_s2,test_s3", GetDiskIds(env));

        // The second fast shard is destroyed, but keeps its devices.
        {
            const auto failure = FailRequest<
                TEvDiskRegistryProxy::TEvDeallocateDevicesRequest,
                TEvDiskRegistryProxy::TEvDeallocateDevicesResponse>(
                env,
                "test_s3");
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, DestroyFileStore(env, "test"));
        }
        UNIT_ASSERT_VALUES_EQUAL("test,test_s1", GetFileStoreIds(env));
        UNIT_ASSERT_VALUES_EQUAL("test_s3", GetDiskIds(env));

        // A retry deallocates them.
        UNIT_ASSERT_VALUES_EQUAL(S_OK, DestroyFileStore(env, "test"));
        UNIT_ASSERT_VALUES_EQUAL("", GetFileStoreIds(env));
        UNIT_ASSERT_VALUES_EQUAL("", GetDiskIds(env));
    }
}

}   // namespace NCloud::NFileStore::NStorage
