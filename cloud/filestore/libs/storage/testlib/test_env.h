#pragma once

#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/diagnostics/metrics/public.h>
#include <cloud/filestore/libs/diagnostics/metrics/registry.h>
#include <cloud/filestore/libs/diagnostics/metrics/visitor.h>
#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/diagnostics/public.h>
#include <cloud/filestore/libs/storage/core/config.h>
#include <cloud/filestore/libs/storage/core/public.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/kikimr/public.h>

#include <ydb/core/base/tabletid.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/test_client.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>
#include <library/cpp/lwtrace/mon/mon_lwtrace.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

#include <functional>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultBlockCount = 268435456;   // TB
constexpr ui32 DefaultChannelCount = 7;

void CheckForkJoin(const NLWTrace::TShuttleTrace& trace, bool forkRequired);

inline auto GetDefaultMaxFileBlocks()
{
    // casting to ui64 is convenient here since some tests use the result of
    // this function to do some arithmetic which may yield values outside of the
    // ui32 domain
    static const ui64 VALUE = TStorageConfig().GetMaxFileBlocks();
    return VALUE;
}

inline auto CompactionRangeToString(
    const NProtoPrivate::TCompactionRangeStats& rs)
{
    return Sprintf(
        "r=%u b=%u d=%u g=%u",
        rs.GetRangeId(),
        rs.GetBlobCount(),
        rs.GetDeletionCount(),
        rs.GetGarbageBlockCount());
}

////////////////////////////////////////////////////////////////////////////////

struct TTestEnvConfig
{
    ui32 DomainUid = 1;
    TString DomainName = "local";

    ui32 StaticNodes = 1;
    ui32 DynamicNodes = 1;

    ui32 ChannelCount = DefaultChannelCount;
    ui32 Groups = 2;

    NActors::NLog::EPriority LogPriority_NFS = NActors::NLog::PRI_TRACE;
    NActors::NLog::EPriority LogPriority_KiKiMR = NActors::NLog::PRI_WARN;
    NActors::NLog::EPriority LogPriority_Others = NActors::NLog::PRI_WARN;
};

////////////////////////////////////////////////////////////////////////////////

class TTestEnv
{
private:
    TTestEnvConfig Config;
    ILoggingServicePtr Logging;
    TStorageConfigPtr StorageConfig;
    TDiagnosticsConfigPtr DiagConfig;
    IProfileLogPtr ProfileLog;
    ITraceSerializerPtr TraceSerializer;

    NKikimr::TTestBasicRuntime Runtime;
    TVector<ui32> GroupIds;
    ui32 NextDynamicNode = 0;
    ui64 NextTabletId = 0;

    TMap<ui64, NKikimr::TTabletStorageInfo*> TabletIdToStorageInfo;

    NMonitoring::TDynamicCountersPtr Counters;
    IRequestStatsRegistryPtr StatsRegistry;

    NMetrics::IMainMetricsRegistryPtr Registry;

public:
    TTestEnv(
        const TTestEnvConfig& config = {},
        NProto::TStorageConfig storageConfig = {},
        NKikimr::NFake::TCaches cachesConfig = {},
        IProfileLogPtr profileLog = CreateProfileLogStub(),
        NProto::TDiagnosticsConfig diagConfig = {});

    NActors::TTestActorRuntime& GetRuntime()
    {
        return Runtime;
    }

    TStorageConfigPtr GetStorageConfig() const
    {
        return StorageConfig;
    }

    ui64 GetHive();
    ui64 GetSchemeShard();

    const auto& GetGroupIds() const
    {
        return GroupIds;
    }

    NMonitoring::TDynamicCountersPtr GetCounters() const
    {
        return Counters;
    }

    NMetrics::IMainMetricsRegistryPtr GetRegistry() const
    {
        return Registry;
    }

    TLog CreateLog();

    ui64 AllocateTxId();
    void CreateSubDomain(const TString& name);
    ui32 CreateNode(const TString& name);

    ui64 BootIndexTablet(ui32 nodeIdx);
    void UpdateTabletStorageInfo(ui64 tabletId, ui32 channelCount);
    TString UpdatePrivateCacheSize(ui64 tabletId, ui64 cacheSize);
    ui64 GetPrivateCacheSize(ui64 tabletId);

private:
    void SetupLogging();

    void SetupDomain(NKikimr::TAppPrepare& app);
    void SetupChannelProfiles(NKikimr::TAppPrepare& app);

    std::unique_ptr<NKikimr::TTabletStorageInfo> BuildIndexTabletStorageInfo(
        ui64 tabletId,
        ui32 channelCount);

    void BootTablets();
    void BootStandardTablet(
        ui64 tabletId,
        NKikimr::TTabletTypes::EType type,
        ui32 nodeIdx = 0);

    void SetupStorage();

    void SetupLocalServices();
    void SetupLocalService(ui32 nodeIdx);

    void SetupLocalServiceConfig(
        NKikimr::TAppData& appData,
        NKikimr::TLocalConfig& localConfig);

    void SetupProxies();
    void SetupTicketParser(ui32 nodeIdx);
    void SetupTxProxy(ui32 nodeIdx);
    void SetupCompileService(ui32 nodeIdx);

    void InitSchemeShard();

    void WaitForSchemeShardTx(ui64 txId);
};

////////////////////////////////////////////////////////////////////////////////

class TTestRegistryVisitor
    : public NMetrics::IRegistryVisitor
{
public:
    using TLabel = NCloud::NFileStore::NMetrics::TLabel;

private:
    struct TMetricsEntry
    {
        TInstant Time;
        NMetrics::EAggregationType AggrType;
        NMetrics::EMetricType MetrType;
        THashMap<TString, TString> Labels;
        i64 Value;

        bool Matches(const TVector<TLabel>& labels) const
        {
            for (auto& label: labels) {
                auto it = Labels.find(label.GetName());
                if (it == Labels.end() || it->second != label.GetValue()) {
                    return false;
                }
            }
            return true;
        }
    };

    TVector<TMetricsEntry> MetricsEntries;
    TMetricsEntry CurrentEntry;

public:
    void OnStreamBegin() override;
    void OnStreamEnd() override;
    void OnMetricBegin(
        TInstant time,
        NMetrics::EAggregationType aggrType,
        NMetrics::EMetricType metrType) override;
    void OnMetricEnd() override;
    void OnLabelsBegin() override;
    void OnLabelsEnd() override;
    void OnLabel(TStringBuf name, TStringBuf value) override;
    void OnValue(i64 value) override;

public:
    const TVector<TMetricsEntry>& GetEntries() const;
    void ValidateExpectedCounters(
        const TVector<std::pair<TVector<TLabel>, i64>>& expectedCounters);
    void ValidateExpectedCountersWithPredicate(
        const TVector<
            std::pair<TVector<TLabel>, std::function<bool(i64)>>
        >& expectedCounters);
    void ValidateExpectedHistogram(
        const TVector<std::pair<TVector<TLabel>, i64>>& expectedCounters,
        bool checkEqual);

private:
    static TString LabelsToString(const TVector<TLabel>& labels);
};

////////////////////////////////////////////////////////////////////////////////

TStorageConfigPtr CreateTestStorageConfig(NProto::TStorageConfig storageConfig);

////////////////////////////////////////////////////////////////////////////////

#define TABLET_TEST_HEAD(name)                                                 \
    void TestImpl##name(TFileSystemConfig tabletConfig);                       \
    Y_UNIT_TEST(name)                                                          \
    {                                                                          \
        TestImpl##name(TFileSystemConfig{.BlockSize = 4_KB});                  \
    }                                                                          \
// TABLET_TEST_HEAD

#define TABLET_TEST_IMPL(name, largeBS)                                        \
    TABLET_TEST_HEAD(name)                                                     \
    Y_UNIT_TEST(name##largeBS)                                                 \
    {                                                                          \
        TestImpl##name(TFileSystemConfig{.BlockSize = largeBS});               \
    }                                                                          \
    void TestImpl##name(TFileSystemConfig tabletConfig)                        \
// TABLET_TEST_IMPL

#define TABLET_TEST_4K_ONLY(name)                                              \
    TABLET_TEST_HEAD(name)                                                     \
    void TestImpl##name(TFileSystemConfig tabletConfig)                        \
// TABLET_TEST_4K_ONLY

#define TABLET_TEST(name)                                                      \
    TABLET_TEST_IMPL(name, 128_KB)                                             \
// TABLET_TEST

#define TABLET_TEST_16K(name)                                                  \
    TABLET_TEST_IMPL(name, 16_KB)                                              \
// TABLET_TEST_16K

}   // namespace NCloud::NFileStore::NStorage
