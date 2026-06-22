#include "part.h"

#include "part_events_private.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/compaction_options.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/libs/storage/partition/model/fresh_blob.h>
#include <cloud/blockstore/libs/storage/partition/model/fresh_blob_test.h>
#include <cloud/blockstore/libs/storage/partition/part.h>
#include <cloud/blockstore/libs/storage/partition/part_events_private.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/testlib/part_client.h>
#include <cloud/blockstore/libs/storage/testlib/test_env.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>
#include <cloud/blockstore/public/api/protos/volume.pb.h>

// TODO: invalid reference
#include <cloud/blockstore/libs/storage/service/service_events_private.h>
#include <cloud/blockstore/libs/storage/volume/volume_events_private.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>
#include <cloud/storage/core/libs/common/helpers.h>
#include <cloud/storage/core/libs/common/sglist_test.h>
#include <cloud/storage/core/libs/tablet/blob_id.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/core/blobstorage/base/blobstorage_events.h>
#include <contrib/ydb/core/blobstorage/vdisk/common/vdisk_events.h>
#include <contrib/ydb/core/mind/bscontroller/bsc.h>
#include <contrib/ydb/core/testlib/basics/storage.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/bitmap.h>
#include <util/generic/size_literals.h>
#include <util/generic/variant.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NStorage;

using namespace NLWTrace;

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_PARTITION_REQUESTS_MON(xxx, ...)                            \
    xxx(RemoteHttpInfo,         __VA_ARGS__)                                   \
// BLOCKSTORE_PARTITION_REQUESTS_MON

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DataChannelOffset = 3;
const TActorId VolumeActorId(0, "VVV");

TString GetBlockContent(char fill = 0, size_t size = DefaultBlockSize)
{
    return TString(size, fill);
}

TString GetBlocksContent(
    char fill = 0,
    ui32 blockCount = 1,
    size_t blockSize = DefaultBlockSize)
{
    TString result;
    for (ui32 i = 0; i < blockCount; ++i) {
        result += GetBlockContent(fill, blockSize);
    }
    return result;
}

void CheckRangesArePartition(
    TVector<TBlockRange32> ranges,
    const TBlockRange32& unionRange)
{
    UNIT_ASSERT(ranges.size() > 0);

    Sort(ranges, [](const auto& l, const auto& r) {
            return l.Start < r.Start;
        });

    const auto* prev = &ranges.front();
    UNIT_ASSERT_VALUES_EQUAL(unionRange.Start, prev->Start);

    for (size_t i = 1; i < ranges.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL_C(prev->End + 1, ranges[i].Start,
            "during iteration #" << i);
        prev = &ranges[i];
    }

    UNIT_ASSERT_VALUES_EQUAL(unionRange.End, ranges.back().End);
}

NProto::TStorageServiceConfig DefaultConfig(ui32 flushBlobSizeThreshold = 4_KB)
{
    NProto::TStorageServiceConfig config;
    config.SetFlushBlobSizeThreshold(flushBlobSizeThreshold);
    config.SetFreshByteCountThresholdForBackpressure(400_KB);
    config.SetFreshByteCountLimitForBackpressure(1200_KB);
    config.SetFreshByteCountFeatureMaxValue(6);
    config.SetCollectGarbageThreshold(10);
    config.SetDiskPrefixLengthWithBlockChecksumsInBlobs(1_GB);

    return config;
}

TDiagnosticsConfigPtr CreateTestDiagnosticsConfig()
{
    NProto::TDiagnosticsConfig config;
    config.SetPassTraceIdToBlobstorage(true);
    return std::make_shared<TDiagnosticsConfig>(std::move(config));
}

////////////////////////////////////////////////////////////////////////////////

struct TTestPartitionInfo
{
    TString DiskId = "test";
    TString BaseDiskId;
    TString BaseDiskCheckpointId;
    ui64 TabletId = TestTabletId;
    ui64 BaseTabletId = 0;
    NCloud::NProto::EStorageMediaKind MediaKind =
        NCloud::NProto::STORAGE_MEDIA_DEFAULT;
    TMaybe<ui32> MaxBlocksInBlob;
};

////////////////////////////////////////////////////////////////////////////////

class TDummyActor final
    : public TActor<TDummyActor>
{
public:
    TDummyActor()
        : TActor(&TThis::StateWork)
    {
    }

private:
    STFUNC(StateWork)
    {
        Y_UNUSED(ev);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestEnv
{
private:
    const ui32 DomainUid = 1;
    const TString DomainName = "local";

    TTestBasicRuntime Runtime;
    TVector<ui32> GroupIds;

public:
    TTestEnv(
            ui32 staticNodes,
            ui32 dynamicNodes,
            ui32 nchannels,
            ui32 ngroups)
        : Runtime(staticNodes + dynamicNodes ? staticNodes + dynamicNodes : 1, false)
    {
        Runtime.AddLocalService(
            VolumeActorId,
            TActorSetupCmd(new TDummyActor, TMailboxType::Simple, 0));
        Runtime.AddLocalService(
            MakeHiveProxyServiceId(),
            TActorSetupCmd(new TDummyActor, TMailboxType::Simple, 0));
        Runtime.AppendToLogSettings(
            TBlockStoreComponents::START,
            TBlockStoreComponents::END,
            GetComponentName);
        for (ui32 i = TBlockStoreComponents::START;
             i < TBlockStoreComponents::END;
             ++i)
        {
            Runtime.SetLogPriority(i, NLog::PRI_INFO);
        }
        Runtime.SetLogPriority(NKikimrServices::BS_NODE, NLog::PRI_ERROR);

        TAppPrepare app;
        SetupDomain(app);
        NKikimr::SetupChannelProfiles(app, DomainUid, nchannels);
        SetupTabletServices(Runtime, &app);
        BootBSController();
        SetupStorage(ngroups);
    }

    TTestActorRuntime& GetRuntime()
    {
        return Runtime;
    }

    const auto& GetGroupIds() const
    {
        return GroupIds;
    }

    void CreateSubDomain(const TString& name)
    {
        Y_UNUSED(name);
    }

    ui32 CreateBlockStoreNode(
        const TString& name,
        TStorageConfigPtr storageConfig,
        TDiagnosticsConfigPtr diagnosticsConfig)
    {
        Y_UNUSED(name);
        Y_UNUSED(storageConfig);
        Y_UNUSED(diagnosticsConfig);
        return 0;
    }

private:
    ui64 ChangeDomain(ui64 tabletId) const
    {
        return MakeTabletID(DomainUid, DomainUid, UniqPartFromTabletID(tabletId));
    }

    void SetupDomain(TAppPrepare& app)
    {
        auto domain = TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(
            DomainName,
            DomainUid,
            ChangeDomain(Tests::SchemeRoot),
            DomainUid,
            DomainUid,
            TVector<ui32>{DomainUid},
            DomainUid,
            TVector<ui32>{DomainUid},
            7,
            TVector<ui64>{TDomainsInfo::MakeTxCoordinatorID(DomainUid, 1)},
            TVector<ui64>{TDomainsInfo::MakeTxMediatorID(DomainUid, 1)},
            TVector<ui64>{TDomainsInfo::MakeTxAllocatorID(DomainUid, 1)},
            DefaultPoolKinds(2));

        app.AddDomain(domain.Release());
    }

    void BootBSController()
    {
        auto bootstrapper = CreateTestBootstrapper(
            Runtime,
            CreateTestTabletInfo(
                MakeBSControllerID(DomainUid),
                TTabletTypes::BSController),
            &CreateFlatBsController);
        Runtime.EnableScheduleForActor(bootstrapper);
    }

    void SetupStorage(ui32 ngroups)
    {
        SetupBoxAndStoragePool(
            Runtime,
            Runtime.AllocateEdgeActor(),
            DomainUid,
            ngroups);

        auto selectGroups =
            std::make_unique<TEvBlobStorage::TEvControllerSelectGroups>();
        const auto& spTypes =
            Runtime.GetAppData().DomainsInfo->GetDomain(DomainUid).StoragePoolTypes;
        for (const auto& x: spTypes) {
            selectGroups->Record.AddGroupParameters()
                ->MutableStoragePoolSpecifier()->SetName(x.second.GetName());
        }
        selectGroups->Record.SetReturnAllMatchingGroups(true);
        Runtime.SendToPipe(
            MakeBSControllerID(DomainUid),
            Runtime.AllocateEdgeActor(),
            selectGroups.release());

        TAutoPtr<IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<
            TEvBlobStorage::TEvControllerSelectGroupsResult>(
                handle,
                TDuration::Seconds(5));
        UNIT_ASSERT(handle);

        std::unique_ptr<TEvBlobStorage::TEvControllerSelectGroupsResult> response(
            handle->Release<TEvBlobStorage::TEvControllerSelectGroupsResult>()
                .Release());

        for (const auto& mg: response->Record.GetMatchingGroups()) {
            for (const auto& g: mg.GetGroups()) {
                GroupIds.push_back(g.GetGroupID());
            }
        }

        UNIT_ASSERT(ngroups <= GroupIds.size());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TEventExecutionOrderFilter
{
public:
    TEventExecutionOrderFilter(
        TTestActorRuntimeBase& runtime,
        const TVector<std::pair<ui32, ui32>>& eventOrders)
        : Runtime(runtime)
    {
        for (const auto& [prerequisiteEvent, dependentEvent]: eventOrders) {
            EventDependencies[dependentEvent] = prerequisiteEvent;
            PrerequisiteEvents.insert(prerequisiteEvent);
        }
    }

    TTestActorRuntimeBase::TEventFilter operator()(
        TTestActorRuntimeBase::TEventFilter baseFilter = nullptr)
    {
        return [this, baseFilter](
                   TTestActorRuntimeBase& rt,
                   TAutoPtr<IEventHandle>& ev) -> bool
        {
            Y_ABORT_UNLESS(ev);

            TActorId recipient = ev->GetRecipientRewrite();
            ui32 eventType = ev->GetTypeRewrite();

            bool baseFilterResult = baseFilter ? baseFilter(rt, ev) : false;

            auto prerequisiteEventIt = EventDependencies.find(eventType);
            if (prerequisiteEventIt != EventDependencies.end()) {
                ui32 prerequisiteEvent = prerequisiteEventIt->second;
                auto& processedEventsByRecipient = ProcessedEvents[recipient];
                if (!processedEventsByRecipient.contains(prerequisiteEvent)) {
                    DelayedEvents[recipient][prerequisiteEvent] = std::move(ev);
                    return true;
                }
            }

            auto& delayedEventsByRecipient = DelayedEvents[recipient];
            auto delayedEventIt = delayedEventsByRecipient.find(eventType);
            if (delayedEventIt != delayedEventsByRecipient.end()) {
                TAutoPtr<IEventHandle> delayedEvent =
                    std::move(delayedEventIt->second);
                delayedEventsByRecipient.erase(delayedEventIt);

                // Remove the recipient from the map if there are no more
                // delayed events
                if (delayedEventsByRecipient.empty()) {
                    DelayedEvents.erase(recipient);
                }

                Runtime.Schedule(delayedEvent, TDuration::MilliSeconds(100));
            }

            if (PrerequisiteEvents.contains(eventType)) {
                ProcessedEvents[recipient].insert(eventType);
            }

            return baseFilterResult;
        };
    }

private:
    TTestActorRuntimeBase& Runtime;
    THashMap<ui32, ui32> EventDependencies;
    THashSet<ui32> PrerequisiteEvents;
    THashMap<TActorId, THashSet<ui32>> ProcessedEvents;
    // Map of delayed events by recipient and event type
    THashMap<TActorId, THashMap<ui32, TAutoPtr<IEventHandle>>> DelayedEvents;
};

////////////////////////////////////////////////////////////////////////////////

void InitTestActorRuntime(
    TTestActorRuntime& runtime,
    const NProto::TStorageServiceConfig& config,
    ui32 blockCount,
    ui32 channelCount,
    std::unique_ptr<TTabletStorageInfo> tabletInfo,
    TTestPartitionInfo partitionInfo = TTestPartitionInfo(),
    EStorageAccessMode storageAccessMode = EStorageAccessMode::Default)
{
    auto storageConfig = std::make_shared<TStorageConfig>(
        config,
        std::make_shared<NFeatures::TFeaturesConfig>(
            NCloud::NProto::TFeaturesConfig())
    );

    NProto::TPartitionConfig partConfig;

    partConfig.SetDiskId(partitionInfo.DiskId);
    partConfig.SetBaseDiskId(partitionInfo.BaseDiskId);
    partConfig.SetBaseDiskCheckpointId(partitionInfo.BaseDiskCheckpointId);
    partConfig.SetBaseDiskTabletId(partitionInfo.BaseTabletId);
    partConfig.SetStorageMediaKind(partitionInfo.MediaKind);

    partConfig.SetBlockSize(DefaultBlockSize);
    partConfig.SetBlocksCount(blockCount);

    if (partitionInfo.MaxBlocksInBlob) {
        partConfig.SetMaxBlocksInBlob(*partitionInfo.MaxBlocksInBlob);
    }

    auto* cps = partConfig.MutableExplicitChannelProfiles();
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::System));
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Log));
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Index));

    for (ui32 i = 0; i < channelCount - DataChannelOffset - 1; ++i) {
        cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
    }

    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Fresh));

    auto diagConfig = CreateTestDiagnosticsConfig();

    auto createFunc =
        [=] (const TActorId& owner, TTabletStorageInfo* info) {
            auto tablet = CreatePartitionTablet(
                owner,
                info,
                storageConfig,
                diagConfig,
                CreateProfileLogStub(),
                CreateBlockDigestGeneratorStub(),
                partConfig,
                storageAccessMode,
                0,  // partitionIndex
                1,  // siblingCount
                VolumeActorId,
                0  // volumeTabletId
            );
            return tablet.release();
        };

    auto bootstrapper = CreateTestBootstrapper(runtime, tabletInfo.release(), createFunc);
    runtime.EnableScheduleForActor(bootstrapper);
}

////////////////////////////////////////////////////////////////////////////////

auto InitTestActorRuntime(
    TTestEnv& env,
    TTestActorRuntime& runtime,
    ui32 channelCount,
    ui32 tabletInfoChannelCount,  // usually should be equal to channelCount
    const NProto::TStorageServiceConfig& config = DefaultConfig())
{
    {
        env.CreateSubDomain("nbs");
        auto storageConfig = CreateTestStorageConfig(config);
        env.CreateBlockStoreNode(
            "nbs",
            storageConfig,
            CreateTestDiagnosticsConfig()
        );
    }

    const auto tabletId = NKikimr::MakeTabletID(1, HiveId, 1);
    std::unique_ptr<TTabletStorageInfo> x(new TTabletStorageInfo());

    x->TabletID = tabletId;
    x->TabletType = TTabletTypes::BlockStorePartition;
    auto& channels = x->Channels;
    channels.resize(tabletInfoChannelCount);

    for (ui64 channel = 0; channel < channels.size(); ++channel) {
        channels[channel].Channel = channel;
        channels[channel].Type =
            TBlobStorageGroupType(BootGroupErasure);
        channels[channel].History.resize(1);
        channels[channel].History[0].FromGeneration = 0;
        const auto gidx =
            channel > DataChannelOffset ? channel - DataChannelOffset : 0;
        channels[channel].History[0].GroupID = env.GetGroupIds()[gidx];
    }

    InitTestActorRuntime(runtime, config, 1024, channelCount, std::move(x));

    return tabletId;
}

////////////////////////////////////////////////////////////////////////////////

void InitLogSettings(TTestActorRuntime& runtime)
{
    for (ui32 i = TBlockStoreComponents::START; i < TBlockStoreComponents::END; ++i) {
        runtime.SetLogPriority(i, NLog::PRI_INFO);
        // runtime.SetLogPriority(i, NLog::PRI_DEBUG);
    }
    // runtime.SetLogPriority(NLog::InvalidComponent, NLog::PRI_DEBUG);
    runtime.SetLogPriority(NKikimrServices::BS_NODE, NLog::PRI_ERROR);
}

std::unique_ptr<TTestActorRuntime> PrepareTestActorRuntime(
    NProto::TStorageServiceConfig config = DefaultConfig(),
    ui32 blockCount = 1024,
    TMaybe<ui32> channelsCount = {},
    const TTestPartitionInfo& testPartitionInfo = TTestPartitionInfo(),
    IActorPtr volumeProxy = {},
    EStorageAccessMode storageAccessMode = EStorageAccessMode::Default)
{
    auto runtime = std::make_unique<TTestBasicRuntime>(1);

    if (volumeProxy) {
        runtime->AddLocalService(
            MakeVolumeProxyServiceId(),
            TActorSetupCmd(volumeProxy.release(), TMailboxType::Simple, 0));
    }

    runtime->AddLocalService(
        VolumeActorId,
        TActorSetupCmd(new TDummyActor, TMailboxType::Simple, 0));

    runtime->AddLocalService(
        MakeHiveProxyServiceId(),
        TActorSetupCmd(new TDummyActor, TMailboxType::Simple, 0));

    runtime->AppendToLogSettings(
        TBlockStoreComponents::START,
        TBlockStoreComponents::END,
        GetComponentName);

    InitLogSettings(*runtime);

    SetupTabletServices(*runtime);

    std::unique_ptr<TTabletStorageInfo> tabletInfo(CreateTestTabletInfo(
        testPartitionInfo.TabletId,
        TTabletTypes::BlockStorePartition));

    if (channelsCount) {
        auto& channels = tabletInfo->Channels;
        channels.resize(*channelsCount);

        for (ui64 i = 0; i < channels.size(); ++i) {
            auto& channel = channels[i];
            channel.History.resize(1);
        }
    }

    InitTestActorRuntime(
        *runtime,
        config,
        blockCount,
        channelsCount ? *channelsCount : tabletInfo->Channels.size(),
        std::move(tabletInfo),
        testPartitionInfo,
        storageAccessMode
    );

    return runtime;
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void AssertEqual(const TVector<T>& l, const TVector<T>& r)
{
    UNIT_ASSERT_VALUES_EQUAL(l.size(), r.size());
    for (size_t i = 0; i < l.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(l[i], r[i]);
    }
}

////////////////////////////////////////////////////////////////////////////////

TString GetBlockContent(
    const std::unique_ptr<TEvService::TEvReadBlocksResponse>& response)
{
    if (response->Record.GetBlocks().BuffersSize() == 1) {
        return response->Record.GetBlocks().GetBuffers(0);
    }
    return {};
}

TString GetBlocksContent(
    const std::unique_ptr<TEvService::TEvReadBlocksResponse>& response)
{
    const auto& blocks = response->Record.GetBlocks();

    {
        bool empty = true;
        for (size_t i = 0; i < blocks.BuffersSize(); ++i) {
            if (blocks.GetBuffers(i)) {
                empty = false;
            }
        }

        if (empty) {
            return TString();
        }
    }

    TString result;

    for (size_t i = 0; i < blocks.BuffersSize(); ++i) {
        const auto& block = blocks.GetBuffers(i);
        if (!block) {
            result += GetBlockContent(char(0));
            continue;
        }
        result += block;
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TDynBitMap GetBitMap(const TString& s)
{
    TDynBitMap mask;

    if (s) {
        mask.Reserve(s.size() * 8);
        Y_ABORT_UNLESS(mask.GetChunkCount() * sizeof(TDynBitMap::TChunk) == s.size());
        auto* dst = const_cast<TDynBitMap::TChunk*>(mask.GetChunks());
        memcpy(dst, s.data(), s.size());
    }

    return mask;
}

TDynBitMap GetUnencryptedBlockMask(
    const std::unique_ptr<TEvService::TEvReadBlocksResponse>& response)
{
    return GetBitMap(response->Record.GetUnencryptedBlockMask());
}

TDynBitMap GetUnencryptedBlockMask(
    const std::unique_ptr<TEvService::TEvReadBlocksLocalResponse>& response)
{
    return GetBitMap(response->Record.GetUnencryptedBlockMask());
}

TDynBitMap CreateBitmap(size_t size)
{
    TDynBitMap bitmap;
    bitmap.Reserve(size);
    bitmap.Set(0, size);
    return bitmap;
}

void MarkZeroedBlocks(TDynBitMap& bitmap, const TBlockRange32& range)
{
    if (range.End >= bitmap.Size()) {
        bitmap.Reserve(range.End);
    }
    bitmap.Set(range.Start, range.End + 1);
}

void MarkWrittenBlocks(TDynBitMap& bitmap, const TBlockRange32& range)
{
    if (range.End >= bitmap.Size()) {
        bitmap.Reserve(range.End);
    }
    bitmap.Reset(range.Start, range.End + 1);
}

////////////////////////////////////////////////////////////////////////////////

TTestActorRuntime::TEventObserver StorageStateChanger(
    ui32  flag,
    TMaybe<ui32> groupIdFilter = {})
{
    return [=] (TAutoPtr<IEventHandle>& event) {
        switch (event->GetTypeRewrite()) {
            case TEvBlobStorage::EvPutResult: {
                auto* msg = event->Get<TEvBlobStorage::TEvPutResult>();
                if (!groupIdFilter.Defined() || *groupIdFilter == msg->GroupId) {
                    const_cast<TStorageStatusFlags&>(msg->StatusFlags).Merge(
                        ui32(NKikimrBlobStorage::StatusIsValid) | ui32(flag)
                    );
                    break;
                }
            }
        }

        return TTestActorRuntime::DefaultObserverFunc(event);
    };
}

TTestActorRuntime::TEventObserver PartitionBatchWriteCollector(TTestActorRuntime& runtime, ui32 eventCount)
{
    bool dropProcessWriteQueue = true;
    ui32 cnt = 0;
    bool batchSeen = false;
    NActors::TActorId partActorId;
    return [=, &runtime] (TAutoPtr<IEventHandle>& event) mutable {
        switch (event->GetTypeRewrite()) {
            case TEvPartitionPrivate::EvProcessWriteQueue: {
                batchSeen = true;
                if (dropProcessWriteQueue) {
                    partActorId = event->Sender;
                    return TTestActorRuntime::EEventAction::DROP;
                }
                break;
            }
            case TEvService::EvWriteBlocksRequest: {
                if (++cnt == eventCount && batchSeen) {
                    dropProcessWriteQueue = false;
                    auto req =
                        std::make_unique<TEvPartitionPrivate::TEvProcessWriteQueue>();
                    runtime.Send(
                        new IEventHandle(
                            partActorId,
                            event->Sender,
                            req.release(),
                            0, // flags
                            0),
                        0);
                }
                break;
            }
        }
        return TTestActorRuntime::DefaultObserverFunc(event);
    };
}

////////////////////////////////////////////////////////////////////////////////

struct TEmpty {};

struct TBlob
{
    TBlob(
        ui32 number,
        ui8 offset,
        ui8 blockCount = 1,
        ui32 channel = 0,
        ui32 generation = 0
    )
        : Number(number)
        , Offset(offset)
        , BlockCount(blockCount)
        , Channel(channel)
        , Generation(generation)
    {}

    ui32 Number;
    ui8 Offset;
    ui8 BlockCount;
    ui32 Channel;
    ui32 Generation;
};

struct TFresh
{
    TFresh(ui8 value)
        : Value(value)
    {}

    ui8 Value;
};

using TBlockDescription = std::variant<TEmpty, TBlob, TFresh>;

using TPartitionContent = TVector<TBlockDescription>;

////////////////////////////////////////////////////////////////////////////////

struct TPartitionWithRuntime
{
    std::unique_ptr<TTestActorRuntime> Runtime;
    std::unique_ptr<TPartitionClient> Partition;
};

////////////////////////////////////////////////////////////////////////////////

class TTestVolumeProxyActor final
    : public TActorBootstrapped<TTestVolumeProxyActor>
{
private:
    ui64 BaseTabletId;
    TString BaseDiskId;
    TString BaseDiskCheckpointId;
    TPartitionContent BasePartitionContent;
    ui32 BlockCount;
    ui32 BaseBlockSize;

public:
    TTestVolumeProxyActor(
        ui64 baseTabletId,
        const TString& baseDiskId,
        const TString& baseDiskCheckpointId,
        const TPartitionContent& basePartitionContent,
        ui32 blockCount,
        ui32 baseBlockSize = DefaultBlockSize);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleDescribeBlocksRequest(
        const TEvVolume::TEvDescribeBlocksRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetUsedBlocksRequest(
        const TEvVolume::TEvGetUsedBlocksRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetChangedBlocksRequest(
        const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleKeepAliveRequest(
        const TEvVolumeProxy::TEvKeepAliveRequest::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TTestVolumeProxyActor::TTestVolumeProxyActor(
        ui64 baseTabletId,
        const TString& baseDiskId,
        const TString& baseDiskCheckpointId,
        const TPartitionContent& basePartitionContent,
        ui32 blockCount,
        ui32 baseBlockSize)
    : BaseTabletId(baseTabletId)
    , BaseDiskId(baseDiskId)
    , BaseDiskCheckpointId(baseDiskCheckpointId)
    , BasePartitionContent(std::move(basePartitionContent))
    , BlockCount(blockCount)
    , BaseBlockSize(baseBlockSize)
{}

void TTestVolumeProxyActor::Bootstrap(const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    Become(&TThis::StateWork);
}

void TTestVolumeProxyActor::HandleDescribeBlocksRequest(
    const TEvVolume::TEvDescribeBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;
    const auto& diskId = record.GetDiskId();
    const auto& checkpoint = record.GetCheckpointId();

    UNIT_ASSERT_VALUES_EQUAL(BaseDiskId, diskId);
    UNIT_ASSERT_VALUES_EQUAL(BaseDiskCheckpointId, checkpoint);

    auto response = std::make_unique<TEvVolume::TEvDescribeBlocksResponse>();
    auto blockIndex = 0;

    for (const auto& descr: BasePartitionContent) {
        if (std::holds_alternative<TBlob>(descr)) {
            const auto& blob = std::get<TBlob>(descr);
            auto& blobPiece = *response->Record.AddBlobPieces();
            NKikimr::TLogoBlobID blobId(
                BaseTabletId,
                blob.Generation,
                blob.Number,
                blob.Channel,
                BaseBlockSize * 0x100,
                0);
            LogoBlobIDFromLogoBlobID(
                blobId,
                blobPiece.MutableBlobId());

            auto* range = blobPiece.AddRanges();
            range->SetBlobOffset(blob.Offset);
            range->SetBlockIndex(blockIndex);
            range->SetBlocksCount(blob.BlockCount);
            blockIndex += blob.BlockCount;
        } else if (std::holds_alternative<TFresh>(descr)) {
            const auto& fresh = std::get<TFresh>(descr);
            auto& freshBlockRange = *response->Record.AddFreshBlockRanges();
            freshBlockRange.SetStartIndex(blockIndex);
            freshBlockRange.SetBlocksCount(1);
            freshBlockRange.SetBlocksContent(
                TString(BaseBlockSize, char(fresh.Value)));
            ++blockIndex;
        } else {
            Y_ABORT_UNLESS(std::holds_alternative<TEmpty>(descr));
            ++blockIndex;
        }
    }

    ctx.Send(ev->Sender, response.release());
}

void TTestVolumeProxyActor::HandleGetUsedBlocksRequest(
    const TEvVolume::TEvGetUsedBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;
    const auto& diskId = record.GetDiskId();

    UNIT_ASSERT_VALUES_EQUAL(BaseDiskId, diskId);

    auto response = std::make_unique<TEvVolume::TEvGetUsedBlocksResponse>();
    ui64 blockIndex = 0;

    TCompressedBitmap bitmap(BlockCount);

    for (const auto& descr: BasePartitionContent) {
        if (std::holds_alternative<TBlob>(descr)) {
            const auto& blob = std::get<TBlob>(descr);
            bitmap.Set(blockIndex, blockIndex + blob.BlockCount);
            blockIndex += blob.BlockCount;
        } else if (std::holds_alternative<TFresh>(descr)) {
            bitmap.Set(blockIndex, blockIndex + 1);
            ++blockIndex;
        } else {
            Y_ABORT_UNLESS(std::holds_alternative<TEmpty>(descr));
            ++blockIndex;
        }
    }

    auto serializer = bitmap.RangeSerializer(0, bitmap.Capacity());
    TCompressedBitmap::TSerializedChunk chunk;
    while (serializer.Next(&chunk)) {
        if (!TCompressedBitmap::IsZeroChunk(chunk)) {
            auto* usedBlock = response->Record.AddUsedBlocks();
            usedBlock->SetChunkIdx(chunk.ChunkIdx);
            usedBlock->SetData(chunk.Data.data(), chunk.Data.size());
        }
    }

    // serialize/deserialize for better testing (to catch the bug NBS-3934)
    auto serialized = response->Record.SerializeAsString();
    UNIT_ASSERT(response->Record.ParseFromString(serialized));

    ctx.Send(ev->Sender, response.release());
}

void TTestVolumeProxyActor::HandleGetChangedBlocksRequest(
    const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& record = msg->Record;

    UNIT_ASSERT_VALUES_EQUAL(BaseDiskId, record.GetDiskId());
    UNIT_ASSERT_VALUES_EQUAL(BaseDiskCheckpointId, record.GetHighCheckpointId());
    UNIT_ASSERT_VALUES_EQUAL("", record.GetLowCheckpointId());

    auto response = std::make_unique<TEvService::TEvGetChangedBlocksResponse>();
    ui64 blockIndex = 0;

    TVector<ui8> changedBlocks((record.GetBlocksCount() + 7) / 8);

    auto fillBlock = [&](ui64 block) {
        if (block < record.GetStartIndex() || block >= record.GetStartIndex() + record.GetBlocksCount()) {
            return;
        }

        ui64 bit = block - record.GetStartIndex();
        changedBlocks[bit / 8] |= 1 << (bit % 8);
    };

    for (const auto& descr: BasePartitionContent) {
        if (std::holds_alternative<TBlob>(descr)) {
            const auto& blob = std::get<TBlob>(descr);
            for (ui64 block = blockIndex; block < blockIndex + blob.BlockCount; block++) {
                fillBlock(block);
            }
            blockIndex += blob.BlockCount;
        } else if (std::holds_alternative<TFresh>(descr)) {
            fillBlock(blockIndex);
            ++blockIndex;
        } else {
            Y_ABORT_UNLESS(std::holds_alternative<TEmpty>(descr));
            ++blockIndex;
        }
    }

    for (const auto& b: changedBlocks) {
        response->Record.MutableMask()->push_back(b);
    }

    ctx.Send(ev->Sender, response.release());
}

void TTestVolumeProxyActor::HandleKeepAliveRequest(
    const TEvVolumeProxy::TEvKeepAliveRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    UNIT_ASSERT_VALUES_EQUAL(BaseDiskId, msg->DiskId);

    ctx.Send(
        ev->Sender,
        std::make_unique<TEvVolumeProxy::TEvKeepAliveResponse>().release());
}

STFUNC(TTestVolumeProxyActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvVolume::TEvDescribeBlocksRequest, HandleDescribeBlocksRequest);
        HFunc(TEvVolume::TEvGetUsedBlocksRequest, HandleGetUsedBlocksRequest);
        HFunc(TEvService::TEvGetChangedBlocksRequest, HandleGetChangedBlocksRequest);
        HFunc(TEvVolumeProxy::TEvKeepAliveRequest, HandleKeepAliveRequest);
        IgnoreFunc(TEvVolume::TEvMapBaseDiskIdToTabletId);
        IgnoreFunc(TEvVolume::TEvClearBaseDiskIdToTabletIdMapping);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::VOLUME_PROXY,
                __PRETTY_FUNCTION__);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

bool EventHandler(
    ui64 tabletId,
    const TEvBlobStorage::TEvGet::TPtr& ev,
    TTestActorRuntimeBase& runtime,
    ui32 blockSize = DefaultBlockSize)
{
    bool result = false;

    auto* msg = ev->Get();

    auto response =
        std::make_unique<TEvBlobStorage::TEvGetResult>(
            NKikimrProto::OK, msg->QuerySize, 0 /* groupId */);

    for (ui32 i = 0; i < msg->QuerySize; ++i) {
        const auto& q = msg->Queries[i];
        const auto& blobId = q.Id;

        UNIT_ASSERT_C(
            !result || blobId.TabletID() == tabletId,
            "All blobs in one TEvGet request should belong to one partition;"
            " tabletId: " << tabletId <<
            " blobId: " << blobId);

        if (blobId.TabletID() == tabletId) {
            result = true;

            auto& r = response->Responses[i];

            r.Id = blobId;
            r.Status = NKikimrProto::OK;

            UNIT_ASSERT(q.Size % blockSize == 0);
            UNIT_ASSERT(q.Shift % blockSize == 0);

            auto blobOffset = q.Shift / blockSize;

            const auto blobEnd = blobOffset + q.Size / blockSize;
            for (; blobOffset < blobEnd; ++blobOffset) {
                UNIT_ASSERT_C(
                    blobOffset <= 0xff,
                    "Blob offset should fit in one byte");

                // Debugging is easier when block content is equal to blob offset.
                r.Buffer.Insert(r.Buffer.End(), TRope(TString(blockSize, char(blobOffset))));
            }
        }
    }

    if (result) {
        runtime.Schedule(
            new IEventHandle(
                ev->Sender,
                ev->Recipient,
                response.release(),
                0,
                ev->Cookie),
            TDuration());
    }
    return result;
}

TPartitionWithRuntime SetupOverlayPartition(
    ui64 overlayTabletId,
    ui64 baseTabletId,
    const TPartitionContent& basePartitionContent = {},
    TMaybe<ui32> channelsCount = {},
    ui32 blockSize = DefaultBlockSize,
    ui32 blockCount = 1024,
    const NProto::TStorageServiceConfig& config = DefaultConfig(),
    TMaybe<ui32> MaxBlocksInBlob = {})
{
    TPartitionWithRuntime result;

    result.Runtime = PrepareTestActorRuntime(
        config,
        blockCount,
        channelsCount,
        {
            "overlay-disk",
            "base-disk",
            "checkpoint",
            overlayTabletId,
            baseTabletId,
            NCloud::NProto::STORAGE_MEDIA_DEFAULT,
            MaxBlocksInBlob
        },
        std::make_unique<TTestVolumeProxyActor>(
            baseTabletId,
            "base-disk",
            "checkpoint",
            basePartitionContent,
            blockCount,
            blockSize));

    bool baseDiskIsMapped = false;
    result.Runtime->SetEventFilter([&]
        (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev)
    {
        Y_UNUSED(runtime);

        if (ev->GetTypeRewrite() == TEvVolume::EvMapBaseDiskIdToTabletId) {
            const auto* msg = ev->Get<TEvVolume::TEvMapBaseDiskIdToTabletId>();

            UNIT_ASSERT_VALUES_EQUAL(msg->BaseDiskId, "base-disk");
            UNIT_ASSERT_VALUES_EQUAL(msg->BaseTabletId, baseTabletId);
            baseDiskIsMapped = true;
        }
        return false;
    });

    result.Partition = std::make_unique<TPartitionClient>(*result.Runtime);
    result.Partition->WaitReady();

    UNIT_ASSERT(baseDiskIsMapped);

    result.Runtime->SetEventFilter([baseTabletId, blockSize] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev) {
        bool handled = false;

        const auto wrapped =
            [&] (const auto& ev) {
                handled = EventHandler(baseTabletId, ev, runtime, blockSize);
            };

        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvGet, wrapped);
        }
        return handled;
    });

    return result;
}

TString GetBlocksContent(
    const TPartitionContent& content,
    size_t blockSize = DefaultBlockSize)
{
    TString result;

    for (const auto& descr: content) {
        if (std::holds_alternative<TEmpty>(descr)) {
            result += TString(blockSize, char(0));
        } else if (std::holds_alternative<TBlob>(descr)) {
            const auto& blob = std::get<TBlob>(descr);
            for (auto i = 0; i < blob.BlockCount; ++i) {
                const auto blobOffset = blob.Offset + i;
                // Debugging is easier when block content is equal to blob offset.
                result += TString(blockSize, char(blobOffset));
            }
        } else if (std::holds_alternative<TFresh>(descr)) {
            const auto& fresh = std::get<TFresh>(descr);
            result += TString(blockSize, char(fresh.Value));
        } else {
            Y_ABORT_UNLESS(false);
        }
    }

    return result;
}

TString BuildRemoteHttpQuery(
    ui64 tabletId,
    const TVector<std::pair<TString, TString>>& keyValues,
    const TString& fragment = "")
{
    auto res = TStringBuilder()
        << "/app?TabletID="
        << tabletId;
    for (const auto& p: keyValues) {
        res << "&" << p.first << "=" << p.second;
    }
    if (fragment) {
        res << "#" << fragment;
    }
    return res;
}

template <typename TRequest>
void SendUndeliverableRequest(
    TTestActorRuntimeBase& runtime,
    TAutoPtr<IEventHandle>& event,
    std::unique_ptr<TRequest> request)
{
    auto fakeRecipient = TActorId(
        event->Recipient.NodeId(),
        event->Recipient.PoolID(),
        0,
        event->Recipient.Hint());
    auto undeliveryActor = event->GetForwardOnNondeliveryRecipient();
    runtime.Send(
        new IEventHandle(
            fakeRecipient,
            event->Sender,
            request.release(),
            event->Flags,
            0,
            &undeliveryActor),
        0);
}

class TMockShuttle: public IShuttle
{
public:
    TMockShuttle(ui64 traceIdx, ui64 spanId)
        : IShuttle(traceIdx, spanId)
    {}

protected:
    bool DoAddProbe(TProbe*, const TParams&, ui64) override
    {
        return true;
    }

    void DoEndOfTrack() override
    {}

    void DoDrop() override
    {}

    void DoSerialize(TShuttleTrace&) override
    {}

    bool DoFork(TShuttlePtr& child) override
    {
        auto shuttle = MakeIntrusive<TMockShuttle>(GetTraceIdx(), GetSpanId());
        shuttle->SetNext(child);
        child = shuttle;
        child->SetParentSpanId(GetSpanId());
        return true;
    }

    bool DoJoin(const TShuttlePtr&) override
    {
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////
