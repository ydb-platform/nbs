#include "service_ut_helpers.h"

#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/disk_registry_proxy/api/service.h>
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

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/map.h>
#include <util/generic/size_literals.h>
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
};

class TTestDiskRegistryProxy final: public TActor<TTestDiskRegistryProxy>
{
private:
    const std::shared_ptr<TTestDiskRegistry> DiskRegistry;

public:
    explicit TTestDiskRegistryProxy(std::shared_ptr<TTestDiskRegistry> diskRegistry)
        : TActor(&TThis::StateWork)
        , DiskRegistry(std::move(diskRegistry))
    {}

private:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvDiskRegistryProxy::TEvAllocateDevicesRequest, HandleAllocateDevices);
            HFunc(TEvDiskRegistryProxy::TEvMarkForCleanupRequest, HandleMarkForCleanup);
            HFunc(TEvDiskRegistryProxy::TEvDeallocateDevicesRequest, HandleDeallocateDevices);
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
        const auto& disk = DiskRegistry->Disks.try_emplace(msg->FileSystemId, *msg).first->second;
        TEvDiskRegistryProxy::TDeviceLayout layout;
        for (ui32 i = 0; i <= disk.ReplicaCount; ++i) {
            layout.Replicas.emplace_back().emplace_back().SetHost("host");
        }

        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskRegistryProxy::TEvAllocateDevicesResponse>(std::move(layout)));
    }

    void HandleMarkForCleanup(
        const TEvDiskRegistryProxy::TEvMarkForCleanupRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvDiskRegistryProxy::TEvMarkForCleanupResponse>(
                CheckOwner(msg->FileSystemId, msg->TabletId)));
    }

    void HandleDeallocateDevices(
        const TEvDiskRegistryProxy::TEvDeallocateDevicesRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        const auto error = CheckOwner(msg->FileSystemId, msg->TabletId);
        if (!HasError(error)) {
            DiskRegistry->Disks.erase(msg->FileSystemId);
        }

        NCloud::Reply(ctx, *ev, std::make_unique<TEvDiskRegistryProxy::TEvDeallocateDevicesResponse>(error));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFastShardTestEnv
{
    const std::shared_ptr<TTestFastShards> Shards = std::make_shared<TTestFastShards>();
    const std::shared_ptr<TTestDiskRegistry> DiskRegistry = std::make_shared<TTestDiskRegistry>();
    TTestEnv Env;
    const ui32 NodeIdx;
    TServiceClient Service;

    explicit TFastShardTestEnv(const NProto::TStorageConfig& config)
        : Env({.FastShardFactory = Shards}, config)
        , NodeIdx(Env.AddDynamicNode())
        , Service(Env.GetRuntime(), NodeIdx)
    {
        auto& runtime = Env.GetRuntime();
        const auto proxyId = runtime.Register(new TTestDiskRegistryProxy(DiskRegistry), NodeIdx);
        runtime.RegisterService(MakeFileStoreDiskRegistryProxyId(), proxyId, NodeIdx);
    }
};

template <typename TResponse, typename... TArgs>
void ReplyInPlace(TAutoPtr<IEventHandle>& event, TArgs&&... args)
{
    event.Reset(new IEventHandle(
        event->Sender,
        event->Recipient,
        new TResponse(std::forward<TArgs>(args)...),
        0,   // flags
        event->Cookie));
}

template <typename TRequest, typename TResponse>
TTestActorRuntimeBase::TEventObserverHolder FailRequest(TFastShardTestEnv& env, TString fileSystemId)
{
    return env.Env.GetRuntime().AddObserver(
        [=, failed = false] (TAutoPtr<IEventHandle>& event) mutable {
            if (!failed && event->GetTypeRewrite() == TRequest::EventType &&
                event->template Get<TRequest>()->FileSystemId == fileSystemId)
            {
                failed = true;
                ReplyInPlace<TResponse>(event, MakeError(E_REJECTED));
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
    auto request = env.Service.CreateResizeFileStoreRequest(id, blocksCount, false /* force */, shardCount);
    request->Record.SetFastShardCount(fastShardCount);
    env.Service.SendResizeFileStoreRequest(std::move(request));
    return env.Service.RecvResizeFileStoreResponse()->GetStatus();
}

auto DestroyFileStore(TFastShardTestEnv& env, const TString& id)
{
    env.Service.SendDestroyFileStoreRequest(id);
    return env.Service.RecvDestroyFileStoreResponse()->GetStatus();
}

void RestartService(TFastShardTestEnv& env, const NProto::TStorageConfig& config)
{
    env.Env.UpdateStorageConfig(config);
    env.Env.CreateAndRegisterStorageService(env.NodeIdx);
}

void RebootTablet(TFastShardTestEnv& env, const TString& id)
{
    const ui64 tabletId = env.Service.GetFileStoreInfo(id)->Record.GetFileStore().GetMainTabletId();
    TIndexTabletClient(env.Env.GetRuntime(), env.NodeIdx, tabletId, {}, false /* updateConfig */).RebootTablet();
}

bool HasDisk(const TFastShardTestEnv& env, const TString& id)
{
    return env.DiskRegistry->Disks.contains(id);
}

TString GetDiskIds(const TFastShardTestEnv& env)
{
    TVector<TString> ids;
    for (const auto& [id, disk]: env.DiskRegistry->Disks) {
        ids.push_back(id);
    }
    return JoinSeq(",", ids);
}

bool HasFileStore(TFastShardTestEnv& env, const TString& id)
{
    return IsIn(env.Service.ListFileStores()->Record.GetFileStores(), id);
}

TString GetFileStoreIds(TFastShardTestEnv& env)
{
    const auto response = env.Service.ListFileStores();
    TVector<TString> ids(response->Record.GetFileStores().begin(), response->Record.GetFileStores().end());
    Sort(ids);
    return JoinSeq(",", ids);
}

TString GetFastShardIds(TFastShardTestEnv& env, const TString& id)
{
    const auto topology = GetFileSystemTopology(env.Service, id);
    TVector<TString> ids;
    for (const auto& shard: topology.GetFastShardTopology().GetShards()) {
        ids.push_back(shard.GetShardId());
    }
    // The files are created in the fast shards.
    UNIT_ASSERT_VALUES_EQUAL(JoinSeq(",", ids), JoinSeq(",", topology.GetFileShardFileSystemIds()));
    return JoinSeq(",", ids);
}

// Starts the fast shards: the tablets answer the requests only after that. Each tablet start makes a new one.
void InitFastShards(TFastShardTestEnv& env, ui32 count)
{
    env.Env.GetRuntime().DispatchEvents({.CustomFinalCondition = [&] {
        return env.Shards->Created.size() >= count;
    }});
    for (const auto& shard: env.Shards->Created) {
        if (!shard->InitResult.HasValue()) {
            shard->InitResult.SetValue({});
        }
    }
}

NProtoPrivate::TPersistentFastShardConfig GetFastShardConfig(const TFastShardTestEnv& env, const TString& id)
{
    NProtoPrivate::TPersistentFastShardConfig config;
    for (const auto& shard: env.Shards->Created) {
        if (shard->FileSystemId == id) {
            config = shard->Config.GetPersistentConfig();
        }
    }
    return config;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TStorageServiceFastShardTest)
{
    Y_UNIT_TEST(ShouldRejectInvalidFastShardRequests)
    {
        auto config = MakeFastShardConfig();
        TFastShardTestEnv env(config);

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            CreateFileStore(env, "test", BlocksCount, 4, 2, static_cast<NProto::EFastShardStorageKind>(2)));
        // The devices of a fast shard have to hold the filesystem.
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            CreateFileStore(env, "test", BlocksCount + 1, 4, 2, NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR));
        // The shard count includes the fast shards, the directories need a shard of their own.
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            CreateFileStore(env, "test", BlocksCount, 2, 2, NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR));
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            CreateFileStore(env, "test", BlocksCount, 1000, 2, NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR));
        UNIT_ASSERT_VALUES_EQUAL("", GetFileStoreIds(env));

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            CreateFileStore(env, "test", BlocksCount, 4, 2, NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR));
        env.Service.CreateFileStore("plain", BlocksCount);
        InitFastShards(env, 2);

        // The fast shards cannot be dropped, they are a part of the shard count and they do not add capacity.
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, ResizeFileStore(env, "test", BlocksCount, 4, 1));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, ResizeFileStore(env, "test", BlocksCount, 4, 4));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, ResizeFileStore(env, "test", 2 * BlocksCount, 6, 3));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, ResizeFileStore(env, "test", 2 * BlocksCount, 0, 0));
        // An ordinary shard cannot become a fast one.
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, ResizeFileStore(env, "test", BlocksCount, 4, 3));
        // Fast shards are created with the filesystem only.
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, ResizeFileStore(env, "plain", BlocksCount, 2, 1));

        // The devices of a new fast shard have to hold the filesystem too.
        config.SetFastShardAllocationSize(AllocationSize / 2);
        RestartService(env, config);
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, ResizeFileStore(env, "test", BlocksCount, 6, 3));

        // A service without the runtime cannot reach disk registry.
        config.SetFastShardRuntimeEnabled(false);
        RestartService(env, config);
        UNIT_ASSERT_VALUES_EQUAL(
            E_PRECONDITION_FAILED,
            CreateFileStore(env, "test2", BlocksCount, 4, 2, NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR));
        UNIT_ASSERT_VALUES_EQUAL(E_PRECONDITION_FAILED, ResizeFileStore(env, "test", BlocksCount, 6, 3));
        UNIT_ASSERT_VALUES_EQUAL(E_PRECONDITION_FAILED, DestroyFileStore(env, "test"));

        // A rejected request changes nothing.
        UNIT_ASSERT_VALUES_EQUAL("plain,test,test_s1,test_s2,test_s3,test_s4", GetFileStoreIds(env));
        UNIT_ASSERT_VALUES_EQUAL("test_s3,test_s4", GetFastShardIds(env, "test"));
        UNIT_ASSERT_VALUES_EQUAL("test_s3,test_s4", GetDiskIds(env));
    }

    Y_UNIT_TEST(ShouldAllocateDevicesForMirrorFastShards)
    {
        TFastShardTestEnv env(MakeFastShardConfig());

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            CreateFileStore(env, "test", BlocksCount / 2, 3, 2, NProto::FAST_SHARD_STORAGE_MIRROR));
        UNIT_ASSERT_VALUES_EQUAL("test_s2,test_s3", GetFastShardIds(env, "test"));
        UNIT_ASSERT_VALUES_EQUAL("test_s2,test_s3", GetDiskIds(env));
        InitFastShards(env, 2);

        // The fast shard tablet owns a disk of the configured size from the configured pool.
        const auto& disk = env.DiskRegistry->Disks.at("test_s3");
        UNIT_ASSERT_VALUES_EQUAL(
            env.Service.GetFileStoreInfo("test_s3")->Record.GetFileStore().GetMainTabletId(),
            disk.TabletId);
        UNIT_ASSERT_EQUAL(NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2, disk.MediaKind);
        UNIT_ASSERT_VALUES_EQUAL(1, disk.ReplicaCount);
        UNIT_ASSERT_VALUES_EQUAL(AllocationSize / 4_KB, disk.DeviceBlocksCount);
        UNIT_ASSERT_VALUES_EQUAL("pool", disk.DevicePoolName);

        // The fast shard gets one device of each replica and room for the whole filesystem.
        const auto config = GetFastShardConfig(env, "test_s3");
        UNIT_ASSERT_VALUES_EQUAL(512_MB, config.GetExpectedGroupCapacity());
        UNIT_ASSERT_VALUES_EQUAL(512, config.GetNodesPerGroup());
        UNIT_ASSERT_EQUAL(NProto::FAST_SHARD_STORAGE_MIRROR, config.GetStorageGroups(0).GetType());
        UNIT_ASSERT_VALUES_EQUAL(2, config.GetStorageGroups(0).DevicesSize());
    }

    Y_UNIT_TEST(ShouldAllocateDevicesForQuorumFastShards)
    {
        TFastShardTestEnv env(MakeFastShardConfig());

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            CreateFileStore(env, "test", BlocksCount, 3, 2, NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR));
        UNIT_ASSERT_VALUES_EQUAL("test_s2,test_s3", GetFastShardIds(env, "test"));
        UNIT_ASSERT_VALUES_EQUAL("test_s2,test_s3", GetDiskIds(env));
        InitFastShards(env, 2);

        // The fast shard tablet owns a disk of the configured size from the configured pool.
        const auto& disk = env.DiskRegistry->Disks.at("test_s3");
        UNIT_ASSERT_VALUES_EQUAL(
            env.Service.GetFileStoreInfo("test_s3")->Record.GetFileStore().GetMainTabletId(),
            disk.TabletId);
        UNIT_ASSERT_EQUAL(NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3, disk.MediaKind);
        UNIT_ASSERT_VALUES_EQUAL(2, disk.ReplicaCount);
        UNIT_ASSERT_VALUES_EQUAL(AllocationSize / 4_KB, disk.DeviceBlocksCount);
        UNIT_ASSERT_VALUES_EQUAL("pool", disk.DevicePoolName);

        // The fast shard gets one device of each replica and room for the whole filesystem.
        const auto config = GetFastShardConfig(env, "test_s3");
        UNIT_ASSERT_VALUES_EQUAL(1_GB, config.GetExpectedGroupCapacity());
        UNIT_ASSERT_VALUES_EQUAL(1024, config.GetNodesPerGroup());
        UNIT_ASSERT_EQUAL(NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR, config.GetStorageGroups(0).GetType());
        UNIT_ASSERT_VALUES_EQUAL(3, config.GetStorageGroups(0).DevicesSize());
    }

    Y_UNIT_TEST(ShouldResizeFileStoreWithFastShards)
    {
        TFastShardTestEnv env(MakeFastShardConfig());

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            CreateFileStore(env, "test", BlocksCount, 4, 2, NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR));
        InitFastShards(env, 2);
        const auto config = GetFastShardConfig(env, "test_s3");

        // A new fast shard gets the storage kind of the filesystem.
        UNIT_ASSERT_VALUES_EQUAL(S_OK, ResizeFileStore(env, "test", BlocksCount, 6, 3));
        UNIT_ASSERT_VALUES_EQUAL("test_s3,test_s4,test_s6", GetFastShardIds(env, "test"));
        UNIT_ASSERT_VALUES_EQUAL("test_s3,test_s4,test_s6", GetDiskIds(env));
        InitFastShards(env, 3);
        UNIT_ASSERT_EQUAL(
            NProto::FAST_SHARD_STORAGE_QUORUM_MIRROR,
            GetFastShardConfig(env, "test_s6").GetStorageGroups(0).GetType());

        // A resize without a fast shard count keeps the fast shards.
        UNIT_ASSERT_VALUES_EQUAL(S_OK, ResizeFileStore(env, "test", BlocksCount, 7, 0));
        UNIT_ASSERT_VALUES_EQUAL("test_s3,test_s4,test_s6", GetFastShardIds(env, "test"));
        UNIT_ASSERT_VALUES_EQUAL("test_s3,test_s4,test_s6", GetDiskIds(env));

        // A fast shard that existed before the resizes starts with the same config.
        RebootTablet(env, "test_s3");
        InitFastShards(env, 4);
        UNIT_ASSERT_VALUES_EQUAL(config.ShortDebugString(), GetFastShardConfig(env, "test_s3").ShortDebugString());
    }

    Y_UNIT_TEST(ShouldRetryFastShardRequestsAfterFailures)
    {
        TFastShardTestEnv env(MakeFastShardConfig());

        // A retry completes a create that failed in the middle of the allocation.
        {
            const auto failure = FailRequest<
                TEvDiskRegistryProxy::TEvAllocateDevicesRequest,
                TEvDiskRegistryProxy::TEvAllocateDevicesResponse>(env, "test_s3");
            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                CreateFileStore(env, "test", BlocksCount, 3, 2, NProto::FAST_SHARD_STORAGE_MIRROR));
        }
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            CreateFileStore(env, "test", BlocksCount, 3, 2, NProto::FAST_SHARD_STORAGE_MIRROR));
        UNIT_ASSERT_VALUES_EQUAL("test_s2,test_s3", GetFastShardIds(env, "test"));
        UNIT_ASSERT_VALUES_EQUAL("test_s2,test_s3", GetDiskIds(env));
        InitFastShards(env, 2);

        // The same for a resize. The filesystem does not use the new shards until the retry.
        {
            const auto failure = FailRequest<
                TEvDiskRegistryProxy::TEvAllocateDevicesRequest,
                TEvDiskRegistryProxy::TEvAllocateDevicesResponse>(env, "test_s5");
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, ResizeFileStore(env, "test", BlocksCount, 5, 3));
        }
        UNIT_ASSERT_VALUES_EQUAL("test_s2,test_s3", GetFastShardIds(env, "test"));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, ResizeFileStore(env, "test", BlocksCount, 5, 3));
        UNIT_ASSERT_VALUES_EQUAL("test_s2,test_s3,test_s5", GetFastShardIds(env, "test"));
        UNIT_ASSERT_VALUES_EQUAL("test_s2,test_s3,test_s5", GetDiskIds(env));
        InitFastShards(env, 3);

        // A shard that is not destroyed keeps its devices: they are deallocated only after it is destroyed.
        {
            const auto failure = FailRequest<
                TEvSSProxy::TEvDestroyFileStoreRequest,
                TEvSSProxy::TEvDestroyFileStoreResponse>(env, "test_s2");
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, DestroyFileStore(env, "test"));
        }
        UNIT_ASSERT(HasFileStore(env, "test"));
        UNIT_ASSERT(HasFileStore(env, "test_s2"));
        UNIT_ASSERT(HasDisk(env, "test_s2"));

        // The shard is destroyed, but its devices are not deallocated.
        {
            const auto failure = FailRequest<
                TEvDiskRegistryProxy::TEvDeallocateDevicesRequest,
                TEvDiskRegistryProxy::TEvDeallocateDevicesResponse>(env, "test_s2");
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, DestroyFileStore(env, "test"));
        }
        UNIT_ASSERT(HasFileStore(env, "test"));
        UNIT_ASSERT(!HasFileStore(env, "test_s2"));
        UNIT_ASSERT(HasDisk(env, "test_s2"));

        // A retry deallocates them as their owner, also after a restart of the main tablet.
        RebootTablet(env, "test");
        UNIT_ASSERT_VALUES_EQUAL(S_OK, DestroyFileStore(env, "test"));
        UNIT_ASSERT_VALUES_EQUAL("", GetFileStoreIds(env));
        UNIT_ASSERT_VALUES_EQUAL("", GetDiskIds(env));
    }
}

}   // namespace NCloud::NFileStore::NStorage
