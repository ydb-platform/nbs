#include "config.h"

#include <cloud/storage/core/protos/certificate.pb.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/hash.h>
#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

#include <google/protobuf/text_format.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TAliases = NProto::TStorageConfig::TFilestoreAliases;

#define FILESTORE_STORAGE_CONFIG(xxx)                                          \
    xxx(SchemeShardDir,                TString,   "/Root"                     )\
                                                                               \
    xxx(PipeClientRetryCount,          ui32,      4                           )\
    xxx(PipeClientMinRetryTime,        TDuration, TDuration::Seconds(1)       )\
    xxx(PipeClientMaxRetryTime,        TDuration, TDuration::Seconds(4)       )\
                                                                               \
    xxx(EstablishSessionTimeout,       TDuration, TDuration::Seconds(30)      )\
    xxx(IdleSessionTimeout,            TDuration, TDuration::Hours(1)         )\
                                                                               \
    xxx(WriteBatchEnabled,             bool,      false                       )\
    xxx(WriteBatchTimeout,             TDuration, TDuration::MilliSeconds(0)  )\
    xxx(WriteBlobThreshold,            ui32,      128_KB                      )\
                                                                               \
    xxx(MaxBlobSize,                        ui32,   4_MB                      )\
    xxx(FlushThreshold,                     ui32,   4_MB                      )\
    xxx(CleanupThreshold,                   ui32,   512                       )\
    xxx(CleanupThresholdAverage,            ui32,   64                        )\
    xxx(CleanupCpuThrottlingThresholdPercentage,                               \
                                            ui32,   0                         )\
    xxx(CalculateCleanupScoreBasedOnUsedBlocksCount,                           \
                                            bool,   false                     )\
    xxx(NewCleanupEnabled,                  bool,   false                     )\
    xxx(CompactionThreshold,                ui32,   20                        )\
    xxx(GarbageCompactionThreshold,         ui32,   100                       )\
    xxx(CompactionThresholdAverage,         ui32,   4                         )\
    xxx(GarbageCompactionThresholdAverage,  ui32,   10                        )\
    xxx(CompactRangeGarbagePercentageThreshold, ui32,    0                    )\
    xxx(CompactRangeAverageBlobSizeThreshold,   ui32,    0                    )\
    xxx(GuestWriteBackCacheEnabled,         bool,   false                     )\
    xxx(NewCompactionEnabled,               bool,   false                     )\
    xxx(UseMixedBlocksInsteadOfAliveBlocksInCompaction, bool,   false         )\
    xxx(CollectGarbageThreshold,            ui32,   4_MB                      )\
    xxx(FlushBytesThreshold,                ui64,   4_MB                      )\
    xxx(FlushBytesItemCountThreshold,       ui32,   100'000                   )\
    xxx(FlushBytesByItemCountEnabled,       bool,   false                     )\
    xxx(MaxDeleteGarbageBlobsPerTx,         ui32,   16384                     )\
    xxx(LoadedCompactionRangesPerTx,        ui32,   10 * 1024 * 1024          )\
    xxx(MaxBlocksPerTruncateTx,             ui32,   0 /*TODO: 32GiB/4KiB*/    )\
    xxx(MaxTruncateTxInflight,              ui32,   10                        )\
                                                                               \
    xxx(AutomaticShardCreationEnabled,                          bool,   false )\
    xxx(ShardAllocationUnit,                                    ui64,   4_TB  )\
    xxx(AutomaticallyCreatedShardSize,                          ui64,   5_TB  )\
    xxx(EnforceCorrectFileSystemShardCountUponSessionCreation,  bool,   false )\
                                                                               \
    xxx(ShardIdSelectionInLeaderEnabled,                        bool,   false )\
    xxx(ShardBalancerDesiredFreeSpaceReserve,                   ui64,   1_TB  )\
    xxx(ShardBalancerMinFreeSpaceReserve,                       ui64,   1_MB  )\
    xxx(ShardBalancerPolicy,                                                   \
            NProto::EShardBalancerPolicy,                                      \
            NProto::SBP_ROUND_ROBIN                                           )\
                                                                               \
    xxx(DirectoryCreationInShardsEnabled,                       bool,   false )\
                                                                               \
    xxx(MaxFileBlocks,                                  ui32,   300_GB / 4_KB )\
    xxx(LargeDeletionMarkersEnabled,                    bool,   false         )\
    xxx(LargeDeletionMarkerBlocks,                      ui64,   1_GB / 4_KB   )\
    xxx(LargeDeletionMarkersThreshold,                  ui64,   128_GB / 4_KB )\
    xxx(LargeDeletionMarkersCleanupThreshold,           ui64,   1_TB / 4_KB   )\
    xxx(LargeDeletionMarkersThresholdForBackpressure,   ui64,   10_TB / 4_KB  )\
                                                                               \
    xxx(CompactionRetryTimeout,             TDuration, TDuration::Seconds(1)  )\
    xxx(BlobIndexOpsPriority,                                                  \
            NProto::EBlobIndexOpsPriority,                                     \
            NProto::BIOP_CLEANUP_FIRST                                        )\
    xxx(EnqueueBlobIndexOpIfNeededScheduleInterval,                            \
                                            TDuration, TDuration::Seconds(1)  )\
                                                                               \
    xxx(FlushThresholdForBackpressure,      ui32,      128_MB                 )\
    xxx(CleanupThresholdForBackpressure,    ui32,      32768                  )\
    xxx(CompactionThresholdForBackpressure, ui32,      200                    )\
    xxx(FlushBytesThresholdForBackpressure, ui64,      128_MB                 )\
    xxx(BackpressureThresholdPercentageForBackgroundOpsPriority,  ui32,   90  )\
                                                                               \
    xxx(HDDSystemChannelPoolKind,      TString,   "rot"                       )\
    xxx(HDDLogChannelPoolKind,         TString,   "rot"                       )\
    xxx(HDDIndexChannelPoolKind,       TString,   "rot"                       )\
    xxx(HDDFreshChannelPoolKind,       TString,   "rot"                       )\
    xxx(HDDMixedChannelPoolKind,       TString,   "rot"                       )\
                                                                               \
    xxx(SSDSystemChannelPoolKind,      TString,   "ssd"                       )\
    xxx(SSDLogChannelPoolKind,         TString,   "ssd"                       )\
    xxx(SSDIndexChannelPoolKind,       TString,   "ssd"                       )\
    xxx(SSDFreshChannelPoolKind,       TString,   "ssd"                       )\
    xxx(SSDMixedChannelPoolKind,       TString,   "ssd"                       )\
                                                                               \
    xxx(HybridSystemChannelPoolKind,   TString,   "ssd"                       )\
    xxx(HybridLogChannelPoolKind,      TString,   "ssd"                       )\
    xxx(HybridIndexChannelPoolKind,    TString,   "ssd"                       )\
    xxx(HybridFreshChannelPoolKind,    TString,   "ssd"                       )\
    xxx(HybridMixedChannelPoolKind,    TString,   "rot"                       )\
                                                                               \
    xxx(AllocationUnitSSD,                 ui32,       32                     )\
    xxx(SSDUnitReadBandwidth,              ui32,       15                     )\
    xxx(SSDUnitWriteBandwidth,             ui32,       15                     )\
    xxx(SSDMaxReadBandwidth,               ui32,       450                    )\
    xxx(SSDMaxWriteBandwidth,              ui32,       450                    )\
    xxx(SSDUnitReadIops,                   ui32,       400                    )\
    xxx(SSDUnitWriteIops,                  ui32,       1000                   )\
    xxx(SSDMaxReadIops,                    ui32,       12000                  )\
    xxx(SSDMaxWriteIops,                   ui32,       40000                  )\
    xxx(SSDThrottlingEnabled,              bool,       false                  )\
    xxx(SSDBoostTime,                      TDuration,  TDuration::Zero()      )\
    xxx(SSDBoostRefillTime,                TDuration,  TDuration::Zero()      )\
    xxx(SSDUnitBoost,                      ui32,       0                      )\
    xxx(SSDBurstPercentage,                ui32,       15                     )\
    xxx(SSDDefaultPostponedRequestWeight,  ui32,       4_KB                   )\
    xxx(SSDMaxPostponedWeight,             ui32,       128_MB                 )\
    xxx(SSDMaxWriteCostMultiplier,         ui32,       10                     )\
    xxx(SSDMaxPostponedTime,               TDuration,  TDuration::Seconds(20) )\
    xxx(SSDMaxPostponedCount,              ui32,       512                    )\
                                                                               \
    xxx(AllocationUnitHDD,                 ui32,       256                    )\
    xxx(HDDUnitReadBandwidth,              ui32,       30                     )\
    xxx(HDDUnitWriteBandwidth,             ui32,       30                     )\
    xxx(HDDMaxReadBandwidth,               ui32,       240                    )\
    xxx(HDDMaxWriteBandwidth,              ui32,       240                    )\
    xxx(HDDUnitReadIops,                   ui32,       100                    )\
    xxx(HDDUnitWriteIops,                  ui32,       300                    )\
    xxx(HDDMaxReadIops,                    ui32,       300                    )\
    xxx(HDDMaxWriteIops,                   ui32,       11000                  )\
    xxx(HDDThrottlingEnabled,              bool,       false                  )\
    xxx(HDDBoostTime,                      TDuration,  TDuration::Minutes(30) )\
    xxx(HDDBoostRefillTime,                TDuration,  TDuration::Hours(12)   )\
    xxx(HDDUnitBoost,                      ui32,       4                      )\
    xxx(HDDBurstPercentage,                ui32,       10                     )\
    xxx(HDDDefaultPostponedRequestWeight,  ui32,       4_KB                   )\
    xxx(HDDMaxPostponedWeight,             ui32,       128_MB                 )\
    xxx(HDDMaxWriteCostMultiplier,         ui32,       20                     )\
    xxx(HDDMaxPostponedTime,               TDuration,  TDuration::Seconds(20) )\
    xxx(HDDMaxPostponedCount,              ui32,       1024                   )\
                                                                               \
    xxx(HDDMediaKindOverride,          ui32,      2        /*HYBRID*/         )\
    xxx(MinChannelCount,               ui32,      4                           )\
                                                                               \
    xxx(DefaultNodesLimit,             ui32,      4194304                     )\
    xxx(SizeToNodesRatio,              ui32,      65536    /*mke2fs huge*/    )\
                                                                               \
    xxx(MaxResponseBytes,              ui32,      4_MB                        )\
    xxx(MaxResponseEntries,            ui32,      1000                        )\
                                                                               \
    xxx(DupCacheEntryCount,            ui32,      1024/*iod 128 but NBS-4016*/)\
    xxx(DisableLocalService,           bool,      false                       )\
    xxx(EnableCollectGarbageAtStart,   bool,      false                       )\
                                                                               \
    xxx(TabletBootInfoBackupFilePath,  TString,   ""                          )\
    xxx(HiveProxyFallbackMode,         bool,      false                       )\
                                                                               \
    xxx(ThrottlingEnabled,             bool,      false                       )\
                                                                               \
    xxx(ReassignChannelsPercentageThreshold,       ui32,      10              )\
                                                                               \
    xxx(CpuLackThreshold,                          ui32,      70              )\
                                                                               \
    xxx(SessionHistoryEntryCount,                  ui32,      100             )\
                                                                               \
    xxx(TenantHiveTabletId,                        ui64,       0              )\
    xxx(FolderId,                                  TString,    {}             )\
    xxx(ConfigsDispatcherServiceEnabled,           bool,      false           )\
    xxx(AuthorizationMode,                                                     \
            NCloud::NProto::EAuthorizationMode,                                \
            NCloud::NProto::AUTHORIZATION_IGNORE                              )\
                                                                               \
    xxx(TwoStageReadEnabled,             bool,      false                     )\
    xxx(TwoStageReadDisabledForHDD,      bool,      false                     )\
    xxx(ThreeStageWriteEnabled,          bool,      false                     )\
    xxx(ThreeStageWriteThreshold,        ui32,      64_KB                     )\
    xxx(ThreeStageWriteDisabledForHDD,   bool,      false                     )\
    xxx(UnalignedThreeStageWriteEnabled, bool,      false                     )\
    xxx(ReadAheadCacheMaxNodes,                 ui32,       1024              )\
    xxx(ReadAheadCacheMaxResultsPerNode,        ui32,       32                )\
    xxx(ReadAheadCacheRangeSize,                ui32,       0                 )\
    xxx(ReadAheadMaxGapPercentage,              ui32,       20                )\
    xxx(ReadAheadCacheMaxHandlesPerNode,        ui32,      128                )\
    xxx(NodeIndexCacheMaxNodes,                 ui32,        0                )\
    xxx(EntryTimeout,                    TDuration, TDuration::Zero()         )\
    xxx(NegativeEntryTimeout,            TDuration, TDuration::Zero()         )\
    xxx(AttrTimeout,                     TDuration, TDuration::Zero()         )\
    xxx(MaxOutOfOrderCompactionMapLoadRequestsInQueue,  ui32,      5          )\
    xxx(MaxBackpressureErrorsBeforeSuicide, ui32,       1000                  )\
    xxx(MaxBackpressurePeriodBeforeSuicide, TDuration,  TDuration::Minutes(10))\
                                                                               \
    xxx(NewLocalDBCompactionPolicyEnabled,              bool,      false      )\
                                                                               \
    xxx(GenerateBlobIdsReleaseCollectBarrierTimeout,                           \
        TDuration,                                                             \
        TDuration::Seconds(10)                                                )\
    xxx(PreferredBlockSizeMultiplier,                   ui32,      1          )\
    xxx(MultiTabletForwardingEnabled,                   bool,      false      )\
    xxx(GetNodeAttrBatchEnabled,                        bool,      false      )\
    xxx(AllowFileStoreForceDestroy,                     bool,      false      )\
    xxx(AllowFileStoreDestroyWithOrphanSessions,        bool,      false      )\
    xxx(TrimBytesItemCount,                             ui64,      100'000    )\
    xxx(NodeRegistrationRootCertsFile,   TString,               {}            )\
    xxx(NodeRegistrationCert,            TCertificate,          {}            )\
    xxx(NodeRegistrationToken,           TString,               "root@builtin")\
    xxx(NodeRegistrationUseSsl,          bool,                  false         )\
    xxx(NodeType,                        TString,               {}            )\
    xxx(BlobCompressionRate,             ui32,                  0             )\
    xxx(BlobCompressionCodec,            TString,               "lz4"         )\
    xxx(BlobCompressionChunkSize,        ui32,                  80_KB         )\
                                                                               \
    xxx(MaxZeroCompactionRangesToDeletePerTx,           ui32,      10000      )\
    xxx(ChannelFreeSpaceThreshold,                      ui32,      25         )\
    xxx(ChannelMinFreeSpace,                            ui32,      10         )\
                                                                               \
    xxx(InMemoryIndexCacheEnabled,                      bool,       false     )\
    xxx(InMemoryIndexCacheNodesCapacity,                ui64,       0         )\
    xxx(InMemoryIndexCacheNodesToNodesCapacityRatio,    ui64,       0         )\
    xxx(InMemoryIndexCacheNodeAttrsCapacity,            ui64,       0         )\
    xxx(InMemoryIndexCacheNodesToNodeAttrsCapacityRatio,ui64,       0         )\
    xxx(InMemoryIndexCacheNodeRefsCapacity,             ui64,       0         )\
    xxx(InMemoryIndexCacheNodesToNodeRefsCapacityRatio, ui64,       0         )\
    xxx(InMemoryIndexCacheLoadOnTabletStart,            bool,       false     )\
    xxx(InMemoryIndexCacheLoadOnTabletStartRowsPerTx,   ui64,       1000      )\
    xxx(InMemoryIndexCacheLoadSchedulePeriod,                                  \
        TDuration,                                                             \
        TDuration::Seconds(0)                                                 )\
                                                                               \
    xxx(NonNetworkMetricsBalancingFactor,               ui32,      1_KB       )\
                                                                               \
    xxx(AsyncDestroyHandleEnabled,     bool,       false                      )\
    xxx(AsyncHandleOperationPeriod,    TDuration,  TDuration::MilliSeconds(50))\
                                                                               \
    xxx(NodeRegistrationMaxAttempts,         ui32,      10                    )\
    xxx(NodeRegistrationTimeout,             TDuration, TDuration::Seconds(5) )\
    xxx(NodeRegistrationErrorTimeout,        TDuration, TDuration::Seconds(1) )\
                                                                               \
    xxx(MultipleStageRequestThrottlingEnabled,          bool,      false      )\
                                                                               \
    xxx(ConfigDispatcherSettings,                                              \
        NCloud::NProto::TConfigDispatcherSettings,                             \
        {}                                                                    )\
                                                                               \
    xxx(PathDescriptionBackupFilePath,  TString,  {}                          )\
                                                                               \
    xxx(DestroyFilestoreDenyList,       TVector<TString>,          {}         )\
                                                                               \
    xxx(SSProxyFallbackMode,            bool,      false                      )\
                                                                               \
    xxx(MixedBlocksOffloadedRangesCapacity,        ui64,     0                )\
    xxx(YdbViewerServiceEnabled,                   bool,     false            )\
    xxx(GuestPageCacheDisabled,                    bool,     false            )\
    xxx(ExtendedAttributesDisabled,                bool,     false            )\
                                                                               \
    xxx(ServerWriteBackCacheEnabled,    bool,      false                      )\
                                                                               \
    xxx(GuestKeepCacheAllowed,                     bool,      false           )\
    xxx(GuestCachingType,                                                      \
        NProto::EGuestCachingType,                                             \
        NProto::GCT_NONE                                                      )\
    xxx(SessionHandleOffloadedStatsCapacity,       ui64,      0               )\
    xxx(LoadConfigsFromCmsRetryMinDelay,   TDuration,  TDuration::Seconds(2)  )\
    xxx(LoadConfigsFromCmsRetryMaxDelay,   TDuration,  TDuration::Seconds(512))\
    xxx(LoadConfigsFromCmsTotalTimeout,    TDuration,  TDuration::Hours(1)    )\
                                                                               \
    xxx(ParentlessFilesOnly,               bool,       false                  )\
// FILESTORE_STORAGE_CONFIG

#define FILESTORE_STORAGE_CONFIG_REF(xxx)                                      \
    xxx(FilestoreAliases,                TAliases,              {}            )\
// FILESTORE_STORAGE_CONFIG_REF

#define FILESTORE_DECLARE_CONFIG(name, type, value)                            \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
// FILESTORE_DECLARE_CONFIG

FILESTORE_STORAGE_CONFIG(FILESTORE_DECLARE_CONFIG)

#undef FILESTORE_DECLARE_CONFIG

////////////////////////////////////////////////////////////////////////////////

template <typename T>
bool IsEmpty(const T& t)
{
    return !t;
}

template <>
bool IsEmpty(const NCloud::NProto::TCertificate& value)
{
    return !value.GetCertFile() && !value.GetCertPrivateKeyFile();
}

template <>
bool IsEmpty(const TAliases& value)
{
    return value.GetEntries().empty();
}

template <typename T>
bool IsEmpty(const google::protobuf::RepeatedPtrField<T>& value)
{
    return value.empty();
}

template <>
bool IsEmpty(const NCloud::NProto::TConfigDispatcherSettings& value)
{
    return !value.HasAllowList() && !value.HasDenyList();
}

template <typename TTarget, typename TSource>
TTarget ConvertValue(const TSource& value)
{
    return static_cast<TTarget>(value);
}

template <>
TCertificate ConvertValue(const NCloud::NProto::TCertificate& value)
{
    return {value.GetCertFile(), value.GetCertPrivateKeyFile()};
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

IOutputStream& operator <<(
    IOutputStream& out,
    NCloud::NProto::EAuthorizationMode mode)
{
    return out << EAuthorizationMode_Name(mode);
}

IOutputStream& operator <<(
    IOutputStream& out,
    NProto::EBlobIndexOpsPriority biopp)
{
    return out << EBlobIndexOpsPriority_Name(biopp);
}

IOutputStream& operator <<(
    IOutputStream& out,
    NProto::EGuestCachingType gct)
{
    return out << EGuestCachingType_Name(gct);
}

IOutputStream& operator <<(
    IOutputStream& out,
    NProto::EShardBalancerPolicy policy)
{
    return out << EShardBalancerPolicy_Name(policy);
}

template <typename T>
void DumpImpl(const T& t, IOutputStream& os)
{
    os << t;
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
void DumpImpl(const TAliases& value, IOutputStream& os)
{
    os << "{ ";
    for (const auto& x: value.GetEntries()) {
        os << x.GetAlias() << ": " << x.GetFsId() << ", ";
    }
    os << " }";
}

template <>
void DumpImpl(const TVector<TString>& value, IOutputStream& os)
{
    for (size_t i = 0; i < value.size(); ++i) {
        if (i) {
            os << ",";
        }
        os << value[i];
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_CONFIG_GETTER(name, type, ...)                               \
type TStorageConfig::Get##name() const                                         \
{                                                                              \
    const auto value = ProtoConfig.Get##name();                                \
    return !IsEmpty(value) ? ConvertValue<type>(value) : Default##name;        \
}                                                                              \
// FILESTORE_CONFIG_GETTER

FILESTORE_STORAGE_CONFIG(FILESTORE_CONFIG_GETTER)

#undef FILESTORE_CONFIG_GETTER

#define FILESTORE_CONFIG_GETTER_REF(name, type, ...)                           \
const type& TStorageConfig::Get##name() const                                  \
{                                                                              \
    return ProtoConfig.Get##name();                                            \
}                                                                              \
// FILESTORE_CONFIG_GETTER_REF

FILESTORE_STORAGE_CONFIG_REF(FILESTORE_CONFIG_GETTER_REF)

#undef FILESTORE_CONFIG_GETTER_REF

void TStorageConfig::Dump(IOutputStream& out) const
{
#define FILESTORE_DUMP_CONFIG(name, ...)                                       \
    out << #name << ": ";                                                      \
    DumpImpl(Get##name(), out);                                                \
    out << Endl;                                                               \
// FILESTORE_DUMP_CONFIG

    FILESTORE_STORAGE_CONFIG(FILESTORE_DUMP_CONFIG);
    FILESTORE_STORAGE_CONFIG_REF(FILESTORE_DUMP_CONFIG);

#undef FILESTORE_DUMP_CONFIG
}

void TStorageConfig::DumpHtml(IOutputStream& out) const
{
#define FILESTORE_DUMP_CONFIG(name, ...)                                       \
    TABLER() {                                                                 \
        TABLED() { out << #name; }                                             \
        TABLED() { DumpImpl(Get##name(), out); }                               \
    }                                                                          \
// FILESTORE_DUMP_CONFIG

    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                FILESTORE_STORAGE_CONFIG(FILESTORE_DUMP_CONFIG);
                FILESTORE_STORAGE_CONFIG_REF(FILESTORE_DUMP_CONFIG);
            }
        }
    }

#undef FILESTORE_DUMP_CONFIG
}

void TStorageConfig::DumpOverridesHtml(IOutputStream& out) const
{
#define FILESTORE_DUMP_CONFIG(name, type, ...) {                               \
    const auto value = ProtoConfig.Get##name();                                \
    if (!IsEmpty(value)) {                                                     \
        TABLER() {                                                             \
            TABLED() { out << #name; }                                         \
            TABLED() { DumpImpl(ConvertValue<type>(value), out); }             \
        }                                                                      \
    }                                                                          \
}                                                                              \
// FILESTORE_DUMP_CONFIG

    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                FILESTORE_STORAGE_CONFIG(FILESTORE_DUMP_CONFIG);
                FILESTORE_STORAGE_CONFIG_REF(FILESTORE_DUMP_CONFIG);
            }
        }
    }

#undef FILESTORE_DUMP_CONFIG
}

////////////////////////////////////////////////////////////////////////////////

void TStorageConfig::Merge(const NProto::TStorageConfig& storageConfig)
{
    ProtoConfig.MergeFrom(storageConfig);
}

TStorageConfig::TValueByName TStorageConfig::GetValueByName(
    const TString& name) const
{
    const auto* descriptor =
        NProto::TStorageConfig::GetDescriptor()->FindFieldByName(name);
    using TStatus = TValueByName::ENameStatus;

    if (descriptor == nullptr) {
        return TValueByName(TStatus::NotFound);
    }

    const auto* reflection = NProto::TStorageConfig::GetReflection();
    if (!reflection->HasField(ProtoConfig, descriptor)) {
        return TValueByName(TStatus::FoundInDefaults);
    }

    TString value;
    google::protobuf::TextFormat::PrintFieldValueToString(
        ProtoConfig,
        descriptor,
        -1,
        &value);

    return TValueByName(value);
}

const NProto::TStorageConfig& TStorageConfig::GetStorageConfigProto() const
{
    return ProtoConfig;
}

const TString* TStorageConfig::FindFileSystemIdByAlias(
    const TString& alias) const
{
    const auto& entries = GetFilestoreAliases().GetEntries();
    for (const auto& entry: entries) {
        if (entry.GetAlias() == alias) {
            return &entry.GetFsId();
        }
    }
    return nullptr;
}

}   // namespace NCloud::NFileStore::NStorage
