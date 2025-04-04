#pragma once

#include <contrib/ydb/core/kqp/common/simple/temp_tables.h>
#include <contrib/ydb/core/kqp/counters/kqp_counters.h>
#include <contrib/ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <contrib/ydb/core/kqp/gateway/kqp_gateway.h>
#include <contrib/ydb/core/protos/config.pb.h>
#include <contrib/ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

#include <contrib/ydb/core/control/immediate_control_board_wrapper.h>
#include <contrib/ydb/library/actors/core/actorid.h>

namespace NKikimr::NKqp {

struct TKqpWorkerSettings {
    TString Cluster;
    TString Database;
    bool LongSession = false;

    NKikimrConfig::TTableServiceConfig TableService;
    NKikimrConfig::TQueryServiceConfig QueryService;

    TControlWrapper MkqlInitialMemoryLimit;
    TControlWrapper MkqlMaxMemoryLimit;

    TKqpDbCountersPtr DbCounters;

    explicit TKqpWorkerSettings(const TString& cluster, const TString& database,
                       const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
                       const  NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
                       TKqpDbCountersPtr dbCounters)
        : Cluster(cluster)
        , Database(database)
        , TableService(tableServiceConfig)
        , QueryService(queryServiceConfig)
        , MkqlInitialMemoryLimit(2097152, 1, Max<i64>())
        , MkqlMaxMemoryLimit(1073741824, 1, Max<i64>())
        , DbCounters(dbCounters)
    {
        AppData()->Icb->RegisterSharedControl(
            MkqlInitialMemoryLimit, "KqpSession.MkqlInitialMemoryLimit");
        AppData()->Icb->RegisterSharedControl(
            MkqlMaxMemoryLimit, "KqpSession.MkqlMaxMemoryLimit");
    }
};

IActor* CreateKqpSessionActor(const TActorId& owner, const TString& sessionId,
    const TKqpSettings::TConstPtr& kqpSettings, const TKqpWorkerSettings& workerSettings,
    std::optional<TKqpFederatedQuerySetup> federatedQuerySetup,
    NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    TIntrusivePtr<TModuleResolverState> moduleResolverState, TIntrusivePtr<TKqpCounters> counters,
    const NKikimrConfig::TQueryServiceConfig& queryServiceConfig,
    const NKikimrConfig::TMetadataProviderConfig& metadataProviderConfig,
    const TActorId& kqpTempTablesAgentActor);

IActor* CreateKqpTempTablesManager(
    TKqpTempTablesState tempTablesState, const TActorId& target, const TString& database);

}  // namespace NKikimr::NKqp
