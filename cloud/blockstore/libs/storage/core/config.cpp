#include "config.h"

#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/libs/features/features_config.h>
#include <cloud/storage/core/protos/certificate.pb.h>

#include <contrib/ydb/core/control/immediate_control_board_impl.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/size_literals.h>

#include <google/protobuf/text_format.h>
#include <google/protobuf/util/message_differencer.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto PreemptedVolumesFile =
    "/var/log/nbs-server/nbs-preempted-volumes.json";

constexpr ui32 DefaultLinkedDiskBandwidth = 100;
constexpr ui32 DefaultLinkedIoDepth = 1;

////////////////////////////////////////////////////////////////////////////////

TDuration Days(ui32 value)
{
    return TDuration::Days(value);
}

TDuration Hours(ui32 value)
{
    return TDuration::Hours(value);
}

TDuration Minutes(ui32 value)
{
    return TDuration::Minutes(value);
}

TDuration Seconds(ui32 value)
{
    return TDuration::Seconds(value);
}

TDuration MSeconds(ui32 value)
{
    return TDuration::MilliSeconds(value);
}

NProto::TLinkedDiskFillBandwidth GetBandwidth(
    const TStorageConfig& config,
    NCloud::NProto::EStorageMediaKind mediaKind)
{
    auto allBandwidths = config.GetLinkedDiskFillBandwidth();

    for (const auto& bandwidth: allBandwidths) {
        if (bandwidth.HasMediaKind() && bandwidth.GetMediaKind() == mediaKind) {
            return bandwidth;
        }
    }

    for (const auto& bandwidth: allBandwidths) {
        if (!bandwidth.HasMediaKind()) {
            return bandwidth;
        }
    }

    NProto::TLinkedDiskFillBandwidth result;
    result.SetReadBandwidth(DefaultLinkedDiskBandwidth);
    result.SetWriteBandwidth(DefaultLinkedDiskBandwidth);
    result.SetReadIoDepth(DefaultLinkedIoDepth);
    result.SetWriteIoDepth(DefaultLinkedIoDepth);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_STORAGE_CONFIG_RO(xxx)                                      \
    xxx(SchemeShardDir,                TString,   "/Root"                     )\
    xxx(DisableLocalService,           bool,      false                       )\
    xxx(ServiceVersionInfo,            TString,   ""                          )\
    xxx(FolderId,                      TString,   ""                          )\
                                                                               \
    xxx(HDDSystemChannelPoolKind,      TString,      "rot"                    )\
    xxx(HDDLogChannelPoolKind,         TString,      "rot"                    )\
    xxx(HDDIndexChannelPoolKind,       TString,      "rot"                    )\
    xxx(HDDMixedChannelPoolKind,       TString,      "rot"                    )\
    xxx(HDDMergedChannelPoolKind,      TString,      "rot"                    )\
    xxx(HDDFreshChannelPoolKind,       TString,      "rot"                    )\
    xxx(SSDSystemChannelPoolKind,      TString,      "ssd"                    )\
    xxx(SSDLogChannelPoolKind,         TString,      "ssd"                    )\
    xxx(SSDIndexChannelPoolKind,       TString,      "ssd"                    )\
    xxx(SSDMixedChannelPoolKind,       TString,      "ssd"                    )\
    xxx(SSDMergedChannelPoolKind,      TString,      "ssd"                    )\
    xxx(SSDFreshChannelPoolKind,       TString,      "ssd"                    )\
    xxx(HybridSystemChannelPoolKind,   TString,      "ssd"                    )\
    xxx(HybridLogChannelPoolKind,      TString,      "ssd"                    )\
    xxx(HybridIndexChannelPoolKind,    TString,      "ssd"                    )\
    xxx(HybridMixedChannelPoolKind,    TString,      "ssd"                    )\
    xxx(HybridMergedChannelPoolKind,   TString,      "rot"                    )\
    xxx(HybridFreshChannelPoolKind,    TString,      "ssd"                    )\
                                                                               \
    /* NBS-2765 */                                                             \
    xxx(CommonSSDPoolKind,             TString,      "ssd"                    )\
    xxx(MaxSSDGroupWriteIops,          ui64,         40000                    )\
    xxx(MaxSSDGroupWriteBandwidth,     ui64,         424 * 1024 * 1024        )\
    xxx(MaxSSDGroupReadIops,           ui64,         12496                    )\
    xxx(MaxSSDGroupReadBandwidth,      ui64,         450 * 1024 * 1024        )\
    xxx(CommonHDDPoolKind,             TString,      "rot"                    )\
    xxx(MaxHDDGroupWriteIops,          ui64,         11000                    )\
    xxx(MaxHDDGroupWriteBandwidth,     ui64,         44 * 1024 * 1024         )\
    xxx(MaxHDDGroupReadIops,           ui64,         292                      )\
    xxx(MaxHDDGroupReadBandwidth,      ui64,         66 * 1024 * 1024         )\
    xxx(CommonOverlayPrefixPoolKind,   TString,      "overlay"                )\
    xxx(NonReplicatedHDDPoolName,      TString,      "rot"                    )\
                                                                               \
    xxx(TabletBootInfoBackupFilePath,   TString,     ""                       )\
    xxx(PathDescriptionBackupFilePath,  TString,     ""                       )\
    xxx(DiskRegistrySplitTransactionCounter, ui32,   10000                    )\
    xxx(DiskRegistryBackupPeriod,      TDuration,    Days(1)                  )\
    xxx(DiskRegistryBackupDirPath,     TString,      ""                       )\
                                                                               \
    xxx(DiskRegistryMetricsCachePeriod, TDuration,   Days(14)                 )\
    xxx(DiskRegistryCountersHost,       TString,     ""                       )\
    xxx(ManuallyPreemptedVolumesFile,   TString,     PreemptedVolumesFile     )\
    xxx(ManuallyPreemptedVolumeLivenessCheckPeriod,  TDuration,   Days(7)     )\
                                                                               \
    xxx(BlobCompressionCodec,           TString,     "lz4"                    )\
                                                                               \
    xxx(KnownSpareNodes,                TVector<TString>,   {}                )\
    xxx(SpareNodeProbability,           ui32,               0                 )\
                                                                               \
    xxx(TenantHiveTabletId,             ui64,               0                 )\
                                                                               \
    xxx(MaxChangedBlocksRangeBlocksCount,           ui64,       1 << 20       )\
    xxx(CachedDiskAgentConfigPath,                  TString,    ""            )\
    xxx(CachedDiskAgentSessionsPath,                TString,    ""            )\
    xxx(ServiceSelfPingInterval,                    TDuration,  MSeconds(10)  )\
                                                                               \
    xxx(DestructionAllowedOnlyForDisksWithIdPrefixes, TVector<TString>, {}    )\
                                                                               \
    xxx(NodeRegistrationToken,                TString,     "root@builtin"     )\
    xxx(NodeRegistrationMaxAttempts,          ui32,        10                 )\
    xxx(NodeRegistrationTimeout,              TDuration,   Seconds(5)         )\
    xxx(NodeRegistrationErrorTimeout,         TDuration,   Seconds(1)         )\
    xxx(NodeType,                             TString,               {}       )\
    xxx(NodeRegistrationRootCertsFile,        TString,               {}       )\
    xxx(NodeRegistrationCert,                 TCertificate,          {}       )\
    xxx(NodeRegistrationUseSsl,               bool,                  false    )\
    xxx(ConfigDispatcherSettings,                                              \
        NCloud::NProto::TConfigDispatcherSettings,                             \
        {}                                                                    )\
    xxx(YdbViewerServiceEnabled,              bool,                  false    )\
    xxx(LinkedDiskFillBandwidth,                                               \
        TVector<NProto::TLinkedDiskFillBandwidth>,                             \
        {}                                                                    )\
// BLOCKSTORE_STORAGE_CONFIG_RO

#define BLOCKSTORE_STORAGE_CONFIG_RW(xxx)                                      \
    xxx(WriteBlobThreshold,            ui32,      1_MB                        )\
    xxx(WriteBlobThresholdSSD,         ui32,      128_KB                      )\
    xxx(WriteMixedBlobThresholdHDD,    ui32,      0                           )\
    xxx(FlushThreshold,                ui32,      4_MB                        )\
    xxx(FreshBlobCountFlushThreshold,  ui32,      3200                        )\
    xxx(FreshBlobByteCountFlushThreshold,   ui32,      16_MB                  )\
                                                                               \
    xxx(SSDCompactionType,                                                     \
            NProto::ECompactionType,                                           \
            NProto::CT_DEFAULT                                                )\
    xxx(HDDCompactionType,                                                     \
            NProto::ECompactionType,                                           \
            NProto::CT_DEFAULT                                                )\
    xxx(CompactionGarbageThreshold,         ui32,      20                     )\
    xxx(CompactionGarbageBlobLimit,         ui32,      100                    )\
    xxx(CompactionGarbageBlockLimit,        ui32,      10240                  )\
    xxx(CompactionRangeGarbageThreshold,    ui32,      200                    )\
    xxx(MaxAffectedBlocksPerCompaction,     ui32,      8192                   )\
    xxx(V1GarbageCompactionEnabled,         bool,      false                  )\
    xxx(OptimizeForShortRanges,             bool,      false                  )\
    xxx(MaxCompactionDelay,                 TDuration, TDuration::Zero()      )\
    xxx(MinCompactionDelay,                 TDuration, TDuration::Zero()      )\
    xxx(MaxCompactionExecTimePerSecond,     TDuration, TDuration::Zero()      )\
    xxx(CompactionScoreHistorySize,             ui32,   10                    )\
    xxx(CompactionScoreLimitForThrottling,      ui32,   300                   )\
    xxx(TargetCompactionBytesPerOp,             ui64,   64_KB                 )\
    xxx(MaxSkippedBlobsDuringCompaction,        ui32,   3                     )\
    xxx(IncrementalCompactionEnabled,           bool,   false                 )\
    xxx(CompactionCountPerRunIncreasingThreshold, ui32, 0                     )\
    xxx(CompactionCountPerRunDecreasingThreshold, ui32, 0                     )\
    xxx(CompactionRangeCountPerRun,             ui32,   3                     )\
    xxx(MaxCompactionRangeCountPerRun,          ui32,   8                     )\
    xxx(GarbageCompactionRangeCountPerRun,      ui32,   1                     )\
    xxx(ForcedCompactionRangeCountPerRun,       ui32,   1                     )\
    xxx(CompactionCountPerRunChangingPeriod,    TDuration, Seconds(60)        )\
    xxx(BatchCompactionEnabled,                 bool,   false                 )\
    xxx(BlobPatchingEnabled,                    bool,   false                 )\
    /* If threshold is not 0, use it */                                        \
    xxx(MaxDiffPercentageForBlobPatching,       ui32,   0                     )\
                                                                               \
    xxx(CleanupThreshold,                       ui32,      10                 )\
    xxx(MaxCleanupDelay,                        TDuration, TDuration::Zero()  )\
    xxx(MinCleanupDelay,                        TDuration, TDuration::Zero()  )\
    xxx(MaxCleanupExecTimePerSecond,            TDuration, TDuration::Zero()  )\
    xxx(CleanupScoreHistorySize,                ui32,      10                 )\
    xxx(CleanupQueueBytesLimitForThrottling,    ui64,      100_MB             )\
    /* measured in overwritten blocks */                                       \
    xxx(UpdateBlobsThreshold,                   ui32,      100                )\
    xxx(MaxBlobsToCleanup,                      ui32,      100                )\
                                                                               \
    xxx(CollectGarbageThreshold,       ui32,      10                          )\
    xxx(RunV2SoftGcAtStartup,                   bool,      false              )\
    xxx(DontEnqueueCollectGarbageUponPartitionStartup,  bool,      false      )\
    xxx(HiveLockExpireTimeout,         TDuration, Seconds(30)                 )\
    xxx(TabletRebootCoolDownIncrement, TDuration, MSeconds(500)               )\
    xxx(TabletRebootCoolDownMax,       TDuration, Seconds(30)                 )\
    xxx(MinExternalBootRequestTimeout, TDuration, Seconds(1)                  )\
    xxx(MaxExternalBootRequestTimeout, TDuration, Seconds(30)                 )\
    xxx(ExternalBootRequestTimeoutIncrement, TDuration, MSeconds(500)         )\
    xxx(PipeClientRetryCount,          ui32,      4                           )\
    xxx(PipeClientMinRetryTime,        TDuration, Seconds(1)                  )\
    xxx(PipeClientMaxRetryTime,        TDuration, Seconds(4)                  )\
    xxx(FlushBlobSizeThreshold,        ui32,      4_MB                        )\
    xxx(FlushToDevNull,                bool,      false                       )\
    xxx(CompactionRetryTimeout,        TDuration, Seconds(1)                  )\
    xxx(CleanupRetryTimeout,           TDuration, Seconds(1)                  )\
    xxx(MaxReadWriteRangeSize,         ui64,      4_GB                        )\
    xxx(MaxBlobRangeSize,              ui32,      128_MB                      )\
    xxx(MaxRangesPerBlob,              ui32,      8                           )\
    xxx(MaxBlobSize,                   ui32,      4_MB                        )\
    xxx(InactiveClientsTimeout,        TDuration, Seconds(9)                  )\
    xxx(AttachedDiskDestructionTimeout,TDuration, Minutes(1)                  )\
    xxx(ClientRemountPeriod,           TDuration, Seconds(4)                  )\
    xxx(InitialAddClientTimeout,       TDuration, Seconds(1)                  )\
    xxx(LocalStartAddClientTimeout,    TDuration, Minutes(1)                  )\
    xxx(MaxIORequestsInFlight,         ui32,      10                          )\
    xxx(MaxIORequestsInFlightSSD,      ui32,      1000                        )\
    xxx(AllowVersionInModifyScheme,    bool,      false                       )\
                                                                               \
    xxx(AllocationUnitSSD,                  ui32,      32                     )\
    xxx(SSDUnitReadBandwidth,               ui32,      15                     )\
    xxx(SSDUnitWriteBandwidth,              ui32,      15                     )\
    xxx(SSDMaxReadBandwidth,                ui32,      450                    )\
    xxx(SSDMaxWriteBandwidth,               ui32,      450                    )\
    xxx(RealSSDUnitReadBandwidth,           ui32,      15                     )\
    xxx(RealSSDUnitWriteBandwidth,          ui32,      15                     )\
    xxx(SSDUnitReadIops,                    ui32,      1000                   )\
    xxx(SSDUnitWriteIops,                   ui32,      1000                   )\
    xxx(SSDMaxReadIops,                     ui32,      20000                  )\
    xxx(SSDMaxWriteIops,                    ui32,      40000                  )\
    xxx(RealSSDUnitReadIops,                ui32,      400                    )\
    xxx(RealSSDUnitWriteIops,               ui32,      1000                   )\
    /* 16000 ReadBlob requests per sec utilize all our cpus                    \
     * We need cpu for at least 2 fully utilized disks                         \
     * CompactionRangeSize / (MaxBandwidth / BlockSize / 8000) = 70            \
     */                                                                        \
    xxx(SSDMaxBlobsPerRange,                ui32,      70                     )\
    xxx(SSDV2MaxBlobsPerRange,              ui32,      20                     )\
                                                                               \
    xxx(AllocationUnitHDD,                  ui32,      256                    )\
    xxx(HDDUnitReadBandwidth,               ui32,      30                     )\
    xxx(HDDUnitWriteBandwidth,              ui32,      30                     )\
    xxx(HDDMaxReadBandwidth,                ui32,      240                    )\
    xxx(HDDMaxWriteBandwidth,               ui32,      240                    )\
    xxx(RealHDDUnitReadBandwidth,           ui32,      2                      )\
    xxx(RealHDDUnitWriteBandwidth,          ui32,      1                      )\
    xxx(HDDUnitReadIops,                    ui32,      300                    )\
    xxx(HDDUnitWriteIops,                   ui32,      300                    )\
    xxx(HDDMaxReadIops,                     ui32,      2000                   )\
    xxx(HDDMaxWriteIops,                    ui32,      11000                  )\
    xxx(RealHDDUnitReadIops,                ui32,      10                     )\
    xxx(RealHDDUnitWriteIops,               ui32,      300                    )\
    /* TODO: properly calculate def value for this param for network-hdd disks \
     */                                                                        \
    xxx(HDDMaxBlobsPerRange,                ui32,      70                     )\
    xxx(HDDV2MaxBlobsPerRange,              ui32,      20                     )\
                                                                               \
    xxx(SSDMaxBlobsPerUnit,                 ui32,      0                      )\
    xxx(HDDMaxBlobsPerUnit,                 ui32,      0                      )\
                                                                               \
    xxx(AllocationUnitNonReplicatedSSD,     ui32,      93                     )\
    xxx(NonReplicatedSSDUnitReadBandwidth,  ui32,      110                    )\
    xxx(NonReplicatedSSDUnitWriteBandwidth, ui32,      82                     )\
    xxx(NonReplicatedSSDMaxReadBandwidth,   ui32,      1024                   )\
    xxx(NonReplicatedSSDMaxWriteBandwidth,  ui32,      1024                   )\
    xxx(NonReplicatedSSDUnitReadIops,       ui32,      28000                  )\
    xxx(NonReplicatedSSDUnitWriteIops,      ui32,      5600                   )\
    xxx(NonReplicatedSSDMaxReadIops,        ui32,      75000                  )\
    xxx(NonReplicatedSSDMaxWriteIops,       ui32,      75000                  )\
                                                                               \
    xxx(AllocationUnitNonReplicatedHDD,     ui32,      93                     )\
    xxx(NonReplicatedHDDUnitReadBandwidth,  ui32,      2                      )\
    xxx(NonReplicatedHDDUnitWriteBandwidth, ui32,      2                      )\
    xxx(NonReplicatedHDDMaxReadBandwidth,   ui32,      200                    )\
    xxx(NonReplicatedHDDMaxWriteBandwidth,  ui32,      200                    )\
    xxx(NonReplicatedHDDUnitReadIops,       ui32,      2                      )\
    xxx(NonReplicatedHDDUnitWriteIops,      ui32,      10                     )\
    xxx(NonReplicatedHDDMaxReadIops,        ui32,      200                    )\
    xxx(NonReplicatedHDDMaxWriteIops,       ui32,      1000                   )\
                                                                               \
    xxx(AllocationUnitMirror2SSD,      ui32,      93                          )\
    xxx(Mirror2SSDUnitReadBandwidth,   ui32,      110                         )\
    xxx(Mirror2SSDUnitWriteBandwidth,  ui32,      82                          )\
    xxx(Mirror2SSDMaxReadBandwidth,    ui32,      1024                        )\
    xxx(Mirror2SSDMaxWriteBandwidth,   ui32,      1024                        )\
    xxx(Mirror2SSDUnitReadIops,        ui32,      28000                       )\
    xxx(Mirror2SSDUnitWriteIops,       ui32,      5600                        )\
    xxx(Mirror2SSDMaxReadIops,         ui32,      75000                       )\
    xxx(Mirror2SSDMaxWriteIops,        ui32,      50000                       )\
                                                                               \
    xxx(Mirror2DiskReplicaCount,       ui32,      1                           )\
                                                                               \
    xxx(AllocationUnitMirror3SSD,      ui32,      93                          )\
    xxx(Mirror3SSDUnitReadBandwidth,   ui32,      110                         )\
    xxx(Mirror3SSDUnitWriteBandwidth,  ui32,      82                          )\
    xxx(Mirror3SSDMaxReadBandwidth,    ui32,      1024                        )\
    xxx(Mirror3SSDMaxWriteBandwidth,   ui32,      1024                        )\
    xxx(Mirror3SSDUnitReadIops,        ui32,      28000                       )\
    xxx(Mirror3SSDUnitWriteIops,       ui32,      5600                        )\
    xxx(Mirror3SSDMaxReadIops,         ui32,      75000                       )\
    xxx(Mirror3SSDMaxWriteIops,        ui32,      40000                       )\
                                                                               \
    xxx(Mirror3DiskReplicaCount,       ui32,      2                           )\
                                                                               \
    xxx(ThrottlingEnabled,             bool,      false                       )\
    xxx(ThrottlingEnabledSSD,          bool,      false                       )\
    xxx(ThrottlingBurstPercentage,     ui32,      100                         )\
    xxx(ThrottlingMaxPostponedWeight,  ui32,      128_MB                      )\
    xxx(DefaultPostponedRequestWeight, ui32,      1_KB                        )\
    xxx(ThrottlingBoostTime,           TDuration, Minutes(30)                 )\
    xxx(ThrottlingBoostRefillTime,     TDuration, Hours(12)                   )\
    xxx(ThrottlingSSDBoostUnits,       ui32,      0                           )\
    xxx(ThrottlingHDDBoostUnits,       ui32,      4                           )\
    xxx(ThrottlerStateWriteInterval,   TDuration, Seconds(60)                 )\
                                                                               \
    xxx(StatsUploadInterval,           TDuration, Seconds(0)                  )\
                                                                               \
    xxx(AuthorizationMode,                                                     \
            NCloud::NProto::EAuthorizationMode,                                \
            NCloud::NProto::AUTHORIZATION_IGNORE                              )\
                                                                               \
    xxx(MaxThrottlerDelay,             TDuration, Seconds(25)                 )\
                                                                               \
    xxx(CompactionScoreLimitForBackpressure,            ui32,   300           )\
    xxx(CompactionScoreThresholdForBackpressure,        ui32,   100           )\
    xxx(CompactionScoreFeatureMaxValue,                 ui32,   10            )\
                                                                               \
    xxx(FreshByteCountLimitForBackpressure,             ui32,   128_MB        )\
    xxx(FreshByteCountThresholdForBackpressure,         ui32,   40_MB         )\
    xxx(FreshByteCountFeatureMaxValue,                  ui32,   10            )\
    xxx(FreshByteCountHardLimit,                        ui32,   256_MB        )\
                                                                               \
    xxx(CleanupQueueBytesLimitForBackpressure,            ui64,   4_TB        )\
    xxx(CleanupQueueBytesThresholdForBackpressure,        ui64,   1_TB        )\
    xxx(CleanupQueueBytesFeatureMaxValue,                 ui32,   10          )\
                                                                               \
    xxx(DiskSpaceScoreThrottlingEnabled,                bool,   false         )\
                                                                               \
    xxx(MaxWriteCostMultiplier,                         ui32,   10            )\
    xxx(ChannelFreeSpaceThreshold,                      ui32,   25            )\
    xxx(ChannelMinFreeSpace,                            ui32,   10            )\
    xxx(MinChannelCount,                                ui32,   4             )\
    xxx(FreshChannelCount,                              ui32,   0             )\
                                                                               \
    xxx(ZoneBlockCount,                            ui32,   32 * MaxBlocksCount)\
    xxx(HotZoneRequestCountFactor,                 ui32,   10                 )\
    xxx(ColdZoneRequestCountFactor,                ui32,   5                  )\
    xxx(BlockListCacheSizePercentage,              ui32,   100                )\
                                                                               \
    xxx(WriteRequestBatchingEnabled,               bool,      false           )\
                                                                               \
    xxx(FreshChannelWriteRequestsEnabled,          bool,      false           )\
                                                                               \
    xxx(AllocateSeparateMixedChannels,                  bool,   false         )\
    xxx(PoolKindChangeAllowed,                          bool,   false         )\
    xxx(MixedChannelsPercentageFromMerged,              ui32,   0             )\
                                                                               \
    xxx(BlockDigestsEnabled,                            bool,   false         )\
    xxx(UseTestBlockDigestGenerator,                    bool,   false         )\
    xxx(DigestedBlocksPercentage,                       ui32,   1             )\
                                                                               \
    xxx(IndexStructuresConversionAttemptInterval,   TDuration,  Seconds(10)   )\
                                                                               \
    xxx(NonReplicatedDiskRecyclingPeriod,           TDuration,  Minutes(10)   )\
    xxx(NonReplicatedDiskRepairTimeout,             TDuration,  Minutes(10)   )\
    xxx(NonReplicatedDiskSwitchToReadOnlyTimeout,   TDuration,  Hours(1)      )\
    xxx(AgentRequestTimeout,                        TDuration,  Seconds(1)    )\
                                                                               \
    xxx(NonReplicatedAgentMinTimeout,                  TDuration,  Seconds(30))\
    xxx(NonReplicatedAgentMaxTimeout,                  TDuration,  Minutes(5) )\
    xxx(NonReplicatedAgentDisconnectRecoveryInterval,  TDuration,  Minutes(1) )\
    xxx(NonReplicatedAgentTimeoutGrowthFactor,         double,     2          )\
                                                                               \
    xxx(AcquireNonReplicatedDevices,                bool,       false         )\
    xxx(VolumePreemptionType,                                                  \
        NProto::EVolumePreemptionType,                                         \
        NProto::PREEMPTION_NONE                                               )\
    xxx(PreemptionPushPercentage,                  ui32,     80               )\
    xxx(PreemptionPullPercentage,                  ui32,     40               )\
                                                                               \
    xxx(DefaultTabletVersion,                      ui32,     0                )\
                                                                               \
    xxx(NonReplicatedInflightLimit,                ui32,        1024          )\
    xxx(MaxTimedOutDeviceStateDuration,            TDuration,   Minutes(20)   )\
                                                                               \
    xxx(MaxDisksInPlacementGroup,                  ui32,      5               )\
    xxx(MaxPlacementPartitionCount,                ui32,      5               )\
    xxx(MaxDisksInPartitionPlacementGroup,         ui32,      32              )\
                                                                               \
    xxx(MaxBrokenHddPlacementGroupPartitionsAfterDeviceRemoval, ui32, 1       )\
                                                                               \
    xxx(BrokenDiskDestructionDelay,                     TDuration, Seconds(5) )\
    xxx(AutomaticallyReplacedDevicesFreezePeriod,       TDuration, Seconds(0) )\
    xxx(MaxAutomaticDeviceReplacementsPerHour,          ui32,      0          )\
                                                                               \
    xxx(VolumeHistoryDuration,                     TDuration, Days(7)         )\
    xxx(VolumeHistoryCacheSize,                    ui32,      100             )\
    xxx(DeletedCheckpointHistoryLifetime,          TDuration, Days(7)         )\
    xxx(VolumeMetaHistoryDisplayedRecordLimit,     ui32,      30              )\
                                                                               \
    xxx(BytesPerPartition,                         ui64,      512_TB          )\
    xxx(BytesPerPartitionSSD,                      ui64,      512_GB          )\
    xxx(BytesPerStripe,                            ui32,      16_MB           )\
    xxx(MaxPartitionsPerVolume,                    ui32,      2               )\
    xxx(MultipartitionVolumesEnabled,              bool,      false           )\
    xxx(NonReplicatedInfraTimeout,                 TDuration, Days(1)         )\
    xxx(NonReplicatedInfraUnavailableAgentTimeout, TDuration, Hours(1)        )\
    xxx(NonReplicatedMinRequestTimeoutSSD,         TDuration, MSeconds(500)   )\
    xxx(NonReplicatedMaxRequestTimeoutSSD,         TDuration, Seconds(30)     )\
    xxx(NonReplicatedMinRequestTimeoutHDD,         TDuration, Seconds(5)      )\
    xxx(NonReplicatedMaxRequestTimeoutHDD,         TDuration, Seconds(30)     )\
    xxx(NonReplicatedMigrationStartAllowed,        bool,      false           )\
    xxx(NonReplicatedVolumeMigrationDisabled,      bool,      false           )\
    xxx(MigrationIndexCachingInterval,             ui32,      65536           )\
    xxx(MaxMigrationBandwidth,                     ui32,      100             )\
    xxx(MaxMigrationIoDepth,                       ui32,      1               )\
    xxx(ExpectedDiskAgentSize,                     ui32,      15              )\
    /* 75 devices = 5 agents */                                                \
    xxx(MaxNonReplicatedDeviceMigrationsInProgress,             ui32,      75 )\
    xxx(MaxNonReplicatedDeviceMigrationPercentageInProgress,    ui32,      5  )\
    xxx(MaxNonReplicatedDeviceMigrationBatchSize,               ui32,    1000 )\
    xxx(MirroredMigrationStartAllowed,             bool,      false           )\
    xxx(PlacementGroupAlertPeriod,                 TDuration, Hours(8)        )\
    xxx(EnableLoadActor,                           bool,      false           )\
                                                                               \
    xxx(UserDataDebugDumpAllowed,                  bool,      false           )\
    xxx(CpuMatBenchNsSystemThreshold,              ui64,      3000            )\
    xxx(CpuMatBenchNsUserThreshold,                ui64,      3000            )\
    xxx(CpuLackThreshold,                          ui32,      70              )\
    xxx(InitialPullDelay,                          TDuration, Minutes(10)     )\
                                                                               \
    xxx(LogicalUsedBlocksUpdateBlockCount,         ui32,      128'000'000     )\
                                                                               \
    xxx(DumpBlockCommitIdsIntoProfileLog,          bool,      false           )\
    xxx(DumpBlobUpdatesIntoProfileLog,             bool,      false           )\
                                                                               \
    /* NBS-2451 */                                                             \
    xxx(EnableConversionIntoMixedIndexV2,          bool,      false           )\
                                                                               \
    xxx(StatsUploadDiskCount,                      ui32,      1000            )\
    xxx(StatsUploadMaxRowsPerTx,                   ui32,      10000           )\
    xxx(StatsUploadRetryTimeout,                   TDuration, Seconds(5)      )\
                                                                               \
    xxx(RemoteMountOnly,                           bool,      false           )\
    xxx(MaxLocalVolumes,                           ui32,      100             )\
                                                                               \
    xxx(DiskRegistryVolumeConfigUpdatePeriod,      TDuration, Minutes(5)      )\
    xxx(DiskRegistryAlwaysAllocatesLocalDisks,     bool,      false           )\
    xxx(DiskRegistryCleanupConfigOnRemoveHost,     bool,      false           )\
                                                                               \
    xxx(ReassignRequestRetryTimeout,               TDuration, Seconds(5)      )\
    xxx(ReassignChannelsPercentageThreshold,       ui32,      10              )\
    xxx(ReassignMixedChannelsPercentageThreshold,  ui32,      100             )\
    xxx(ReassignSystemChannelsImmediately,         bool,      false           )\
    xxx(ReassignFreshChannelsPercentageThreshold,  ui32,      100             )\
                                                                               \
    xxx(MixedIndexCacheV1Enabled,                  bool,      false           )\
    xxx(MixedIndexCacheV1SizeSSD,                  ui32,      32 * 1024       )\
                                                                               \
    xxx(MaxReadBlobErrorsBeforeSuicide,            ui32,      5               )\
    xxx(MaxWriteBlobErrorsBeforeSuicide,           ui32,      1               )\
    xxx(RejectMountOnAddClientTimeout,             bool,      false           )\
    xxx(NonReplicatedVolumeNotificationTimeout,    TDuration, Seconds(30)     )\
    xxx(NonReplicatedSecureEraseTimeout,           TDuration, Minutes(10)     )\
    xxx(MaxDevicesToErasePerDeviceNameForDefaultPoolKind,   ui32,   100       )\
    xxx(MaxDevicesToErasePerDeviceNameForLocalPoolKind,     ui32,   100       )\
    xxx(MaxDevicesToErasePerDeviceNameForGlobalPoolKind,    ui32,   1         )\
                                                                               \
    xxx(HiveProxyFallbackMode,                     bool,      false           )\
    xxx(SSProxyFallbackMode,                       bool,      false           )\
    xxx(UseSchemeCache,                            bool,      false           )\
    xxx(DontPassSchemeShardDirWhenRegisteringNodeInEmergencyMode, bool, false )\
                                                                               \
    xxx(RdmaTargetPort,                            ui32,      10020           )\
    xxx(UseNonreplicatedRdmaActor,                 bool,      false           )\
    xxx(UseRdma,                                   bool,      false           )\
    xxx(NonReplicatedDontSuspendDevices,           bool,      false           )\
    xxx(AddClientRetryTimeoutIncrement,            TDuration, MSeconds(100)   )\
    xxx(MaxNonReplicatedDiskDeallocationRequests,  ui32,      16              )\
    xxx(BalancerActionDelayInterval,               TDuration, Seconds(3)      )\
                                                                               \
    xxx(UseMirrorResync,                           bool,      false           )\
    xxx(ForceMirrorResync,                         bool,      false           )\
    xxx(ResyncIndexCachingInterval,                ui32,      65536           )\
    xxx(ResyncAfterClientInactivityInterval,       TDuration, Minutes(1)      )\
    xxx(AutoResyncPolicy,                                                      \
        NProto::EResyncPolicy,                                                 \
        NProto::EResyncPolicy::RESYNC_POLICY_MINOR_AND_MAJOR_4MB              )\
    xxx(ForceResyncPolicy,                                                     \
        NProto::EResyncPolicy,                                                 \
        NProto::EResyncPolicy::RESYNC_POLICY_MINOR_AND_MAJOR_4MB              )\
    xxx(MirrorReadReplicaCount,                    ui32,      0               )\
                                                                               \
    xxx(PingMetricsHalfDecayInterval,              TDuration, Seconds(15)     )\
    xxx(DisableManuallyPreemptedVolumesTracking,   bool,      false           )\
                                                                               \
    xxx(DisableStartPartitionsForGc,               bool,      false           )\
                                                                               \
    xxx(AddingUnconfirmedBlobsEnabled,             bool,      false           )\
                                                                               \
    xxx(BlobCompressionRate,                       ui32,      0               )\
    xxx(SerialNumberValidationEnabled,             bool,      false           )\
                                                                               \
    xxx(RejectLateRequestsAtDiskAgentEnabled,      bool,      false           )\
    xxx(AssignIdToWriteAndZeroRequestsEnabled,     bool,      false           )\
                                                                               \
    xxx(AgentListExpiredParamsCleanupInterval,     TDuration, Seconds(1)      )\
                                                                               \
    xxx(UseShadowDisksForNonreplDiskCheckpoints,   bool,      false           )\
                                                                               \
    xxx(DiskPrefixLengthWithBlockChecksumsInBlobs, ui64,      0               )\
    xxx(CheckBlockChecksumsInBlobsUponRead,        bool,      false           )\
    xxx(ConfigsDispatcherServiceEnabled,           bool,      false           )\
    xxx(CachedAcquireRequestLifetime,              TDuration, Seconds(40)     )\
                                                                               \
    xxx(UnconfirmedBlobCountHardLimit,             ui32,      1000            )\
                                                                               \
    xxx(VolumeProxyCacheRetryDuration,             TDuration, Seconds(15)     )\
                                                                               \
    xxx(UseDirectCopyRange,                             bool,      false         )\
    xxx(NonReplicatedVolumeDirectAcquireEnabled,        bool,      false         )\
    xxx(MaxShadowDiskFillBandwidth,                     ui32,      512           )\
    xxx(MaxShadowDiskFillIoDepth,                       ui32,      1             )\
    xxx(BackgroundOperationsTotalBandwidth,             ui32,      1024          )\
    xxx(MinAcquireShadowDiskRetryDelayWhenBlocked,      TDuration, MSeconds(250) )\
    xxx(MaxAcquireShadowDiskRetryDelayWhenBlocked,      TDuration, Seconds(1)    )\
    xxx(MinAcquireShadowDiskRetryDelayWhenNonBlocked,   TDuration, Seconds(1)    )\
    xxx(MaxAcquireShadowDiskRetryDelayWhenNonBlocked,   TDuration, Seconds(10)   )\
    xxx(MaxAcquireShadowDiskTotalTimeoutWhenBlocked,    TDuration, Seconds(5)    )\
    xxx(MaxAcquireShadowDiskTotalTimeoutWhenNonBlocked, TDuration, Seconds(600)  )\
    xxx(WaitDependentDisksRetryRequestDelay,            TDuration, Seconds(1)    )\
                                                                                  \
    xxx(DataScrubbingEnabled,                           bool,      false         )\
    xxx(ResyncRangeAfterScrubbing,                      bool,      false         )\
    xxx(ScrubbingInterval,                              TDuration, MSeconds(50)  )\
    xxx(ScrubbingChecksumMismatchTimeout,               TDuration, Seconds(300)  )\
    xxx(ScrubbingBandwidth,                             ui64,      20            )\
    xxx(MaxScrubbingBandwidth,                          ui64,      50            )\
    xxx(MinScrubbingBandwidth,                          ui64,      5             )\
    xxx(AutomaticallyEnableBufferCopyingAfterChecksumMismatch, bool, false       )\
    xxx(ScrubbingResyncPolicy,                                                    \
        NProto::EResyncPolicy,                                                    \
        NProto::EResyncPolicy::RESYNC_POLICY_MINOR_4MB                           )\
                                                                                  \
    xxx(LaggingDevicesForMirror2DisksEnabled,     bool,      false               )\
    xxx(LaggingDevicesForMirror3DisksEnabled,     bool,      false               )\
    xxx(LaggingDeviceTimeoutThreshold,            TDuration, Seconds(5)          )\
    xxx(LaggingDevicePingInterval,                TDuration, MSeconds(500)       )\
    xxx(LaggingDeviceMaxMigrationBandwidth,       ui32,      400                 )\
    xxx(LaggingDeviceMaxMigrationIoDepth,         ui32,      1                   )\
    xxx(ResyncAfterLaggingAgentMigration,         bool,      false               )\
    xxx(MultiAgentWriteEnabled,                   bool,      false               )\
    xxx(MultiAgentWriteRequestSizeThreshold,      ui32,      0                   )\
    xxx(NetworkForwardingTimeout,                 TDuration, MSeconds(100)       )\
                                                                                  \
    xxx(OptimizeVoidBuffersTransferForReadsEnabled,     bool,      false         )\
    xxx(VolumeHistoryCleanupItemCount,                  ui32,      100'000       )\
    xxx(IdleAgentDeployByCmsDelay,                      TDuration, Hours(1)      )\
    xxx(AllowLiteDiskReallocations,                     bool,      false         )\
    xxx(DiskRegistryDisksNotificationTimeout,           TDuration, Seconds(5)    )\
    xxx(BlobStorageAsyncRequestTimeoutHDD,              TDuration, Seconds(0)    )\
    xxx(BlobStorageAsyncRequestTimeoutSSD,              TDuration, Seconds(0)    )\
                                                                               \
    xxx(EncryptionAtRestForDiskRegistryBasedDisksEnabled, bool,    false      )\
    xxx(DisableFullPlacementGroupCountCalculation,        bool,    false      )\
    xxx(DiskRegistryInitialAgentRejectionThreshold,       double,    50       )\
    xxx(EnableToChangeStatesFromDiskRegistryMonpage,      bool,    false      )\
    xxx(EnableToChangeErrorStatesFromDiskRegistryMonpage, bool,    false      )\
    xxx(CalculateSplittedUsedQuotaMetric,                 bool,    false      )\
                                                                               \
    xxx(DestroyVolumeTimeout,                      TDuration, Seconds(30)     )\
    xxx(CompactionMergedBlobThresholdHDD,          ui32,      0               )\
                                                                               \
    xxx(BSGroupsPerChannelToWarmup,                ui32,      1               )\
    xxx(WarmupBSGroupConnectionsTimeout,           TDuration, Seconds(1)      )\
    xxx(AllowAdditionalSystemTablets,              bool,      false           )\
                                                                               \
    xxx(CheckRangeMaxRangeSize,                    ui32,      4_MB            )\
                                                                               \
    xxx(DisableZeroBlocksThrottlingForYDBBasedDisks,       bool,   false      )\
                                                                               \
    xxx(LocalDiskAsyncDeallocationEnabled,                 bool,   false      )\
    xxx(DoNotStopVolumeTabletOnLockLost,                   bool,   false      )\
                                                                               \
    xxx(LoadConfigsFromCmsRetryMinDelay,      TDuration,   Seconds(2)         )\
    xxx(LoadConfigsFromCmsRetryMaxDelay,      TDuration,   Seconds(512)       )\
    xxx(LoadConfigsFromCmsTotalTimeout,       TDuration,   Hours(1)           )\
                                                                               \
    xxx(MaxCompactionRangesLoadingPerTx,                   ui32,   0          )\
    xxx(MaxOutOfOrderCompactionMapChunksInflight,          ui32,   5          )\
                                                                               \
    xxx(PartitionBootTimeout,                 TDuration,   Seconds(0)         )\
    xxx(DirectWriteBandwidthQuota,            ui64,        0                  )\
    xxx(UsePullSchemeForVolumeStatistics,                  bool,   false      )\
    xxx(InitialRetryDelayForServiceRequests,  TDuration,   MSeconds(500)      )\
    xxx(MaxRetryDelayForServiceRequests,      TDuration,   Seconds(30)        )\
    xxx(VolumeThrottlingManagerEnabled,             bool,        false        )\
    xxx(VolumeThrottlingManagerNotificationPeriodSeconds,                      \
        TDuration,                                                             \
        Seconds(5)                                                            )\
    xxx(RetryAcquireReleaseDiskInitialDelay,  TDuration,   MSeconds(100)      )\
    xxx(RetryAcquireReleaseDiskMaxDelay,      TDuration,   Seconds(5)         )\
                                                                               \
    xxx(NonReplicatedVolumeAcquireDiskAfterAddClientEnabled, bool,   false    )\
    xxx(EnableDataIntegrityValidationForYdbBasedDisks,       bool,   false    )\
                                                                               \
    xxx(TrimFreshLogTimeout,                  TDuration,   Seconds(0)         )\

// BLOCKSTORE_STORAGE_CONFIG_RW

#define BLOCKSTORE_STORAGE_CONFIG(xxx)                                         \
    BLOCKSTORE_STORAGE_CONFIG_RO(xxx)                                          \
    BLOCKSTORE_STORAGE_CONFIG_RW(xxx)                                          \
// BLOCKSTORE_STORAGE_CONFIG

#define BLOCKSTORE_STORAGE_DECLARE_CONFIG(name, type, value)                   \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
// BLOCKSTORE_STORAGE_DECLARE_CONFIG

BLOCKSTORE_STORAGE_CONFIG(BLOCKSTORE_STORAGE_DECLARE_CONFIG)

#undef BLOCKSTORE_STORAGE_DECLARE_CONFIG

#define BLOCKSTORE_BINARY_FEATURES(xxx)                                        \
    xxx(Balancer)                                                              \
    xxx(IncrementalCompaction)                                                 \
    xxx(MultipartitionVolumes)                                                 \
    xxx(AllocateFreshChannel)                                                  \
    xxx(FreshChannelWriteRequests)                                             \
    xxx(MixedIndexCacheV1)                                                     \
    xxx(BatchCompaction)                                                       \
    xxx(BlobPatching)                                                          \
    xxx(UseRdma)                                                               \
    xxx(ChangeThrottlingPolicy)                                                \
    xxx(ReplaceDevice)                                                         \
    xxx(UseNonReplicatedHDDInsteadOfReplicated)                                \
    xxx(AddingUnconfirmedBlobs)                                                \
    xxx(EncryptionAtRestForDiskRegistryBasedDisks)                             \
    xxx(LaggingDevicesForMirror2Disks)                                         \
    xxx(LaggingDevicesForMirror3Disks)                                         \

// BLOCKSTORE_BINARY_FEATURES

#define BLOCKSTORE_DURATION_FEATURES(xxx)                                      \
    xxx(MaxTimedOutDeviceStateDuration)                                        \

// BLOCKSTORE_DURATION_FEATURES

#define BLOCKSTORE_STRING_FEATURES(xxx)                                        \
    xxx(SSDSystemChannelPoolKind                                              )\
    xxx(SSDLogChannelPoolKind                                                 )\
    xxx(SSDIndexChannelPoolKind                                               )\
    xxx(SSDFreshChannelPoolKind                                               )\

// BLOCKSTORE_STRING_FEATURES

////////////////////////////////////////////////////////////////////////////////

template <typename TTarget, typename TSource>
TTarget ConvertValue(const TSource& value)
{
    return static_cast<TTarget>(value);
}

template <>
TDuration ConvertValue<TDuration, ui64>(const ui64& value)
{
    return TDuration::MilliSeconds(value);
}

template <>
TDuration ConvertValue<TDuration, ui32>(const ui32& value)
{
    return TDuration::MilliSeconds(value);
}

template <>
TVector<TString> ConvertValue(
    const google::protobuf::RepeatedPtrField<TString>& value)
{
    return TVector<TString>(value.begin(), value.end());
}

template <>
TCertificate ConvertValue(const NCloud::NProto::TCertificate& value)
{
    return {value.GetCertFile(), value.GetCertPrivateKeyFile()};
}

template <>
TVector<NProto::TLinkedDiskFillBandwidth> ConvertValue(
    const google::protobuf::RepeatedPtrField<NProto::TLinkedDiskFillBandwidth>&
        value)
{
    TVector<NProto::TLinkedDiskFillBandwidth> v;
    for (const auto& x: value) {
        v.push_back(x);
    }
    return v;
}

////////////////////////////////////////////////////////////////////////////////

IOutputStream& operator <<(
    IOutputStream& out,
    NCloud::NProto::EAuthorizationMode mode)
{
    return out << EAuthorizationMode_Name(mode);
}

IOutputStream& operator <<(
    IOutputStream& out,
    NProto::ECompactionType ct)
{
    return out << ECompactionType_Name(ct);
}

IOutputStream& operator <<(
    IOutputStream& out,
    NProto::EVolumePreemptionType pt)
{
    return out << EVolumePreemptionType_Name(pt);
}

IOutputStream& operator<<(IOutputStream& out, NProto::EResyncPolicy pt)
{
    return out << NProto::EResyncPolicy_Name(pt);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void DumpImpl(const T& t, IOutputStream& os)
{
    os << t;
}

void DumpImpl(const TVector<TString>& value, IOutputStream& os)
{
    for (size_t i = 0; i < value.size(); ++i) {
        if (i) {
            os << ",";
        }
        os << value[i];
    }
}

template <>
void DumpImpl(const TCertificate& value, IOutputStream& os)
{
    os << "{ "
        << value.CertFile
        << ", "
        << value.CertPrivateKeyFile
        << " }";
}

template <>
void DumpImpl(
    const TVector<NProto::TLinkedDiskFillBandwidth>& value,
    IOutputStream& os)
{
    for (size_t i = 0; i < value.size(); ++i) {
        if (i) {
            os << ",<br>";
        }
        os << value[i];
    }
}

////////////////////////////////////////////////////////////////////////////////

#define CONFIG_ITEM_IS_SET_CHECKER(name, ...)                                  \
    template <typename TProto>                                                 \
    [[nodiscard]] bool Is##name##Set(const TProto& proto)                      \
    {                                                                          \
        if constexpr (requires() { proto.name##Size(); }) {                    \
            return proto.name##Size() > 0;                                     \
        } else {                                                               \
            return proto.Has##name();                                          \
        }                                                                      \
    }

BLOCKSTORE_STORAGE_CONFIG(CONFIG_ITEM_IS_SET_CHECKER);

#undef CONFIG_ITEM_IS_SET_CHECKER

#define BLOCKSTORE_CONFIG_GET_CONFIG_VALUE(config, name, type, value) \
    (Is##name##Set(config) ? ConvertValue<type>(config.Get##name()) : value)

////////////////////////////////////////////////////////////////////////////////

template <typename TValue>
constexpr TAtomicBase ConvertToAtomicBase(const TValue& value)
{
    static_assert(std::is_nothrow_convertible<TValue, TAtomicBase>::value);
    return value;
}

template <>
constexpr TAtomicBase ConvertToAtomicBase(const TDuration& value)
{
    return value.MilliSeconds();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TStorageConfig::TImpl
{
    NProto::TStorageServiceConfig StorageServiceConfig;
    NFeatures::TFeaturesConfigPtr FeaturesConfig;

#define BLOCKSTORE_CONFIG_CONTROL(name, type, value)                           \
    TControlWrapper Control##name{                                             \
        ConvertToAtomicBase<type>(BLOCKSTORE_CONFIG_GET_CONFIG_VALUE(          \
            StorageServiceConfig,                                              \
            name,                                                              \
            type,                                                              \
            value)),                                                           \
        0,                                                                     \
        Max()};                                                                \
    // BLOCKSTORE_CONFIG_CONTROL

    BLOCKSTORE_STORAGE_CONFIG_RW(BLOCKSTORE_CONFIG_CONTROL)

#undef BLOCKSTORE_CONFIG_CONTROL

    TImpl(
            NProto::TStorageServiceConfig storageServiceConfig,
            NFeatures::TFeaturesConfigPtr featuresConfig)
        : StorageServiceConfig(std::move(storageServiceConfig))
        , FeaturesConfig(std::move(featuresConfig))
    {}

    void SetFeaturesConfig(NFeatures::TFeaturesConfigPtr featuresConfig)
    {
        FeaturesConfig = std::move(featuresConfig);
    }

    void SetVolumePreemptionType(
        NProto::EVolumePreemptionType volumePreemptionType)
    {
        StorageServiceConfig.SetVolumePreemptionType(volumePreemptionType);
    }

    NProto::TStorageServiceConfig GetStorageConfigProto() const
    {
        NProto::TStorageServiceConfig proto = StorageServiceConfig;

    // Overriding fields with values from ICB
#define BLOCKSTORE_CONFIG_COPY(name, type, ...)                                \
    if (const ui64 value = Control##name;                                      \
        ConvertValue<type>(value) != ConvertValue<type>(proto.Get##name()))    \
    {                                                                          \
        using T = decltype(StorageServiceConfig.Get##name());                  \
        proto.Set##name(static_cast<T>(value));                                \
    }                                                                          \
    // BLOCKSTORE_CONFIG_COPY

        BLOCKSTORE_STORAGE_CONFIG_RW(BLOCKSTORE_CONFIG_COPY)

#undef BLOCKSTORE_CONFIG_COPY

        return proto;
    }
};

////////////////////////////////////////////////////////////////////////////////

TStorageConfig::TStorageConfig(
        NProto::TStorageServiceConfig storageServiceConfig,
        NFeatures::TFeaturesConfigPtr featuresConfig)
    : Impl(new TImpl(std::move(storageServiceConfig), std::move(featuresConfig)))
{}

TStorageConfig::~TStorageConfig() = default;

void TStorageConfig::SetFeaturesConfig(
    NFeatures::TFeaturesConfigPtr featuresConfig)
{
    Impl->SetFeaturesConfig(std::move(featuresConfig));
}

void TStorageConfig::SetVolumePreemptionType(
    NProto::EVolumePreemptionType volumePreemptionType)
{
    Impl->SetVolumePreemptionType(volumePreemptionType);
}

void TStorageConfig::Register(TControlBoard& controlBoard){
#define BLOCKSTORE_CONFIG_CONTROL(name, type, value)                           \
    controlBoard.RegisterSharedControl(                                        \
        Impl->Control##name,                                                   \
        "BlockStore_" #name);                                                  \
// BLOCKSTORE_CONFIG_CONTROL

    BLOCKSTORE_STORAGE_CONFIG_RW(BLOCKSTORE_CONFIG_CONTROL)

#undef BLOCKSTORE_CONFIG_CONTROL
}

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
    type TStorageConfig::Get##name() const                                     \
    {                                                                          \
        return BLOCKSTORE_CONFIG_GET_CONFIG_VALUE(                             \
            Impl->StorageServiceConfig,                                        \
            name,                                                              \
            type,                                                              \
            Default##name);                                                    \
    }

BLOCKSTORE_STORAGE_CONFIG_RO(BLOCKSTORE_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
    type TStorageConfig::Get##name() const                                     \
    {                                                                          \
        const type configValue = BLOCKSTORE_CONFIG_GET_CONFIG_VALUE(           \
            Impl->StorageServiceConfig,                                        \
            name,                                                              \
            type,                                                              \
            Default##name);                                                    \
        const ui64 rawControlValue = Impl->Control##name;                      \
        const type controlValue = ConvertValue<type>(rawControlValue);         \
        if (controlValue != configValue) {                                     \
            return controlValue;                                               \
        }                                                                      \
        return configValue;                                                    \
    }                                                                          \
    // BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_STORAGE_CONFIG_RW(BLOCKSTORE_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

void TStorageConfig::Dump(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    out << #name << ": ";                                                      \
    DumpImpl(Get##name(), out);                                                \
    out << Endl;                                                               \
// BLOCKSTORE_CONFIG_DUMP

    BLOCKSTORE_STORAGE_CONFIG(BLOCKSTORE_CONFIG_DUMP);

#undef BLOCKSTORE_CONFIG_DUMP
}

void TStorageConfig::DumpHtml(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    TABLER() {                                                                 \
        TABLED() { out << #name; }                                             \
        TABLED() { DumpImpl(Get##name(), out); }                               \
    }                                                                          \
// BLOCKSTORE_CONFIG_DUMP

    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                BLOCKSTORE_STORAGE_CONFIG(BLOCKSTORE_CONFIG_DUMP);
            }
        }
    }

#undef BLOCKSTORE_CONFIG_DUMP
}

#define BLOCKSTORE_BINARY_FEATURE_GETTER(name)                                 \
bool TStorageConfig::Is##name##FeatureEnabled(                                 \
    const TString& cloudId,                                                    \
    const TString& folderId,                                                   \
    const TString& diskId) const                                               \
{                                                                              \
    return Impl->FeaturesConfig->IsFeatureEnabled(                             \
        cloudId,                                                               \
        folderId,                                                              \
        diskId,                                                                \
        #name);                                                                \
}                                                                              \

// BLOCKSTORE_BINARY_FEATURE_GETTER

    BLOCKSTORE_BINARY_FEATURES(BLOCKSTORE_BINARY_FEATURE_GETTER)

#undef BLOCKSTORE_BINARY_FEATURE_GETTER

#define BLOCKSTORE_DURATION_FEATURE_GETTER(name)                               \
TDuration TStorageConfig::Get##name##FeatureValue(                             \
    const TString& cloudId,                                                    \
    const TString& folderId,                                                   \
    const TString& diskId) const                                               \
{                                                                              \
    const auto v = Impl->FeaturesConfig->GetFeatureValue(                      \
        cloudId,                                                               \
        folderId,                                                              \
        diskId,                                                                \
        #name);                                                                \
    if (v) {                                                                   \
        return TDuration::Parse(v);                                            \
    }                                                                          \
                                                                               \
    return TDuration::Zero();                                                  \
}                                                                              \

// BLOCKSTORE_DURATION_FEATURE_GETTER

    BLOCKSTORE_DURATION_FEATURES(BLOCKSTORE_DURATION_FEATURE_GETTER)

#undef BLOCKSTORE_DURATION_FEATURE_GETTER

#define BLOCKSTORE_STRING_FEATURE_GETTER(name)                                 \
TString TStorageConfig::Get##name##FeatureValue(                               \
    const TString& cloudId,                                                    \
    const TString& folderId,                                                   \
    const TString& diskId) const                                               \
{                                                                              \
    return Impl->FeaturesConfig->GetFeatureValue(                              \
        cloudId,                                                               \
        folderId,                                                              \
        diskId,                                                                \
        #name);                                                                \
}                                                                              \

// BLOCKSTORE_STRING_FEATURE_GETTER

    BLOCKSTORE_STRING_FEATURES(BLOCKSTORE_STRING_FEATURE_GETTER)

#undef BLOCKSTORE_STRING_FEATURE_GETTER

TStorageConfigPtr TStorageConfig::Merge(
    TStorageConfigPtr config,
    const NProto::TStorageServiceConfig& patch)
{
    const auto configProto = config->GetStorageConfigProto();
    auto patchedConfigProto = configProto;
    patchedConfigProto.MergeFrom(patch);
    if (google::protobuf::util::MessageDifferencer::Equals(
            patchedConfigProto,
            configProto))
    {
        return config;
    }

    return std::make_shared<TStorageConfig>(
        std::move(patchedConfigProto),
        config->Impl->FeaturesConfig);
}

ui64 GetAllocationUnit(
    const TStorageConfig& config,
    NCloud::NProto::EStorageMediaKind mediaKind)
{
    using namespace NCloud::NProto;

    ui64 unit = 0;
    switch (mediaKind) {
        case STORAGE_MEDIA_SSD:
            unit = config.GetAllocationUnitSSD() * 1_GB;
            break;

        case STORAGE_MEDIA_SSD_NONREPLICATED:
            unit = config.GetAllocationUnitNonReplicatedSSD() * 1_GB;
            break;

        case STORAGE_MEDIA_HDD_NONREPLICATED:
            unit = config.GetAllocationUnitNonReplicatedHDD() * 1_GB;
            break;

        case STORAGE_MEDIA_SSD_MIRROR2:
            unit = config.GetAllocationUnitMirror2SSD() * 1_GB;
            break;

        case STORAGE_MEDIA_SSD_MIRROR3:
            unit = config.GetAllocationUnitMirror3SSD() * 1_GB;
            break;

        case STORAGE_MEDIA_SSD_LOCAL:
        case STORAGE_MEDIA_HDD_LOCAL:
            unit = 4_KB;    // custom pool can have any size
            break;

        default:
            unit = config.GetAllocationUnitHDD() * 1_GB;
            break;
    }

    Y_ABORT_UNLESS(unit != 0); // TODO: this check should be moved to nbs startup

    return unit;
}

TStorageConfig::TValueByName TStorageConfig::GetValueByName(
    const TString& name) const
{
    auto descriptor =
        Impl->StorageServiceConfig.GetDescriptor()->FindFieldByName(name);
    using TStatus = TStorageConfig::TValueByName::ENameStatus;

    if (descriptor == nullptr) {
        return TStorageConfig::TValueByName {TStatus::NotFound};
    }

    const auto* reflection = NProto::TStorageServiceConfig::GetReflection();
    if (!reflection->HasField(Impl->StorageServiceConfig, descriptor)) {
        return TStorageConfig::TValueByName {TStatus::FoundInDefaults};
    }

    TString value;
    google::protobuf::TextFormat::PrintFieldValueToString(
        Impl->StorageServiceConfig,
        descriptor,
        -1,
        &value
    );

    return {value};
}

NProto::TStorageServiceConfig TStorageConfig::GetStorageConfigProto() const
{
    return Impl->GetStorageConfigProto();
}

void AdaptNodeRegistrationParams(
    const TString& overriddenNodeType,
    const NProto::TServerConfig& serverConfig,
    NProto::TStorageServiceConfig& storageConfig)
{
    if (!storageConfig.HasNodeRegistrationMaxAttempts() &&
        serverConfig.HasNodeRegistrationMaxAttempts())
    {
        storageConfig.SetNodeRegistrationMaxAttempts(
            serverConfig.GetNodeRegistrationMaxAttempts());
    }

    if (!storageConfig.HasNodeRegistrationTimeout() &&
        serverConfig.HasNodeRegistrationTimeout())
    {
        storageConfig.SetNodeRegistrationTimeout(
            serverConfig.GetNodeRegistrationTimeout());
    }

    if (!storageConfig.HasNodeRegistrationErrorTimeout() &&
        serverConfig.HasNodeRegistrationErrorTimeout())
    {
        storageConfig.SetNodeRegistrationErrorTimeout(
            serverConfig.GetNodeRegistrationErrorTimeout());
    }

    if (!storageConfig.HasNodeRegistrationToken() &&
        serverConfig.HasNodeRegistrationToken())
    {
        storageConfig.SetNodeRegistrationToken(
            serverConfig.GetNodeRegistrationToken());
    }

    if (overriddenNodeType) {
        storageConfig.SetNodeType(overriddenNodeType);
    }

    if (!storageConfig.HasNodeType() && serverConfig.HasNodeType()) {
        storageConfig.SetNodeType(serverConfig.GetNodeType());
    }
}

TLinkedDiskFillBandwidth GetLinkedDiskFillBandwidth(
    const TStorageConfig& config,
    NCloud::NProto::EStorageMediaKind leaderMediaKind,
    NCloud::NProto::EStorageMediaKind followerMediaKind)
{
    const auto leaderBandwidth = GetBandwidth(config, leaderMediaKind);
    const auto followerBandwidth = GetBandwidth(config, followerMediaKind);

    return TLinkedDiskFillBandwidth{
        .Bandwidth =
            Min(leaderBandwidth.GetReadBandwidth(),
                followerBandwidth.GetWriteBandwidth()),
        .IoDepth =
            Min(leaderBandwidth.GetReadIoDepth(),
                followerBandwidth.GetWriteIoDepth())};
}

}   // namespace NCloud::NBlockStore::NStorage
