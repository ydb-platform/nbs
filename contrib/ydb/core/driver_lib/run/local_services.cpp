#include "kikimr_services_initializers.h"
#include <contrib/ydb/core/blob_depot/blob_depot.h>
#include <contrib/ydb/core/kesus/tablet/tablet.h>
#include <contrib/ydb/core/keyvalue/keyvalue.h>
#include <contrib/ydb/core/mind/labels_maintainer.h>
#include <contrib/ydb/core/mind/tenant_pool.h>
#include <contrib/ydb/core/mind/hive/hive.h>
#include <contrib/ydb/core/persqueue/pq.h>
#include <contrib/ydb/core/statistics/aggregator/aggregator.h>
#include <contrib/ydb/core/sys_view/processor/processor.h>
#include <contrib/ydb/core/test_tablet/test_tablet.h>
#include <contrib/ydb/core/test_tablet/state_server_interface.h>
#include <contrib/ydb/core/tx/coordinator/coordinator.h>
#include <contrib/ydb/core/tx/datashard/datashard.h>
#include <contrib/ydb/core/tx/mediator/mediator.h>
#include <contrib/ydb/core/tx/replication/controller/controller.h>
#include <contrib/ydb/core/tx/schemeshard/schemeshard.h>
#include <contrib/ydb/core/tx/sequenceshard/sequenceshard.h>
#include <contrib/ydb/core/tx/columnshard/columnshard.h>
#include <contrib/ydb/core/backup/controller/tablet.h>
#include <contrib/ydb/core/graph/api/shard.h>

namespace NKikimr::NKikimrServicesInitializers {

TLocalServiceInitializer::TLocalServiceInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{}

TIntrusivePtr<TLocalConfig> TLocalServiceInitializer::BuildLocalConfig(const NKikimr::TAppData* appData) const {
    // choose pool id for important tablets
    ui32 importantPoolId = appData->UserPoolId;
    if (Config.GetFeatureFlags().GetImportantTabletsUseSystemPool()) {
        importantPoolId = appData->SystemPoolId;
    }

    // setup local
    TLocalConfig::TPtr localConfig(new TLocalConfig());

    std::unordered_map<TTabletTypes::EType, NKikimrLocal::TTabletAvailability> tabletAvailabilities;
    for (const auto& availability : Config.GetDynamicNodeConfig().GetTabletAvailability()) {
        tabletAvailabilities.emplace(availability.GetType(), availability);
    }

    auto addToLocalConfig = [&localConfig, &tabletAvailabilities, tabletPool = appData->SystemPoolId](TTabletTypes::EType tabletType,
                                                                                                      TTabletSetupInfo::TTabletCreationFunc op,
                                                                                                      NActors::TMailboxType::EType mailboxType,
                                                                                                      ui32 poolId) {
        auto availIt = tabletAvailabilities.find(tabletType);
        auto localIt = localConfig->TabletClassInfo.emplace(tabletType, new TTabletSetupInfo(op, mailboxType, poolId, TMailboxType::ReadAsFilled, tabletPool)).first;
        if (availIt != tabletAvailabilities.end()) {
            localIt->second.MaxCount = availIt->second.GetMaxCount();
            localIt->second.Priority = availIt->second.GetPriority();
        }
    };

    addToLocalConfig(TTabletTypes::SchemeShard, &CreateFlatTxSchemeShard, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::DataShard, &CreateDataShard, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::KeyValue, &CreateKeyValueFlat, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::PersQueue, &CreatePersQueue, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::PersQueueReadBalancer, &CreatePersQueueReadBalancer, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::Coordinator, &CreateFlatTxCoordinator, TMailboxType::Revolving, importantPoolId);
    addToLocalConfig(TTabletTypes::Mediator, &CreateTxMediator, TMailboxType::Revolving, importantPoolId);
    addToLocalConfig(TTabletTypes::Kesus, &NKesus::CreateKesusTablet, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::Hive, &CreateDefaultHive, TMailboxType::ReadAsFilled, importantPoolId);
    addToLocalConfig(TTabletTypes::SysViewProcessor, &NSysView::CreateSysViewProcessor, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::TestShard, &NTestShard::CreateTestShard, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::ColumnShard, &CreateColumnShard, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::SequenceShard, &NSequenceShard::CreateSequenceShard, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::ReplicationController, &NReplication::CreateController, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::BlobDepot, &NBlobDepot::CreateBlobDepot, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::StatisticsAggregator, &NStat::CreateStatisticsAggregator, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::GraphShard, &NGraph::CreateGraphShard, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::BackupController, &NBackup::CreateBackupController, TMailboxType::ReadAsFilled, appData->UserPoolId);

    return localConfig;
}

void TLocalServiceInitializer::InitializeServices(
        NActors::TActorSystemSetup* setup,
        const NKikimr::TAppData* appData) {
    auto localConfig = BuildLocalConfig(appData);

    TTenantPoolConfig::TPtr tenantPoolConfig = new TTenantPoolConfig(Config.GetTenantPoolConfig(), localConfig);
    if (!tenantPoolConfig->IsEnabled && !tenantPoolConfig->StaticSlots.empty())
        Y_ABORT("Tenant slots are not allowed in disabled pool");

    setup->LocalServices.push_back(std::make_pair(MakeTenantPoolRootID(),
        TActorSetupCmd(CreateTenantPool(tenantPoolConfig), TMailboxType::ReadAsFilled, 0)));

    setup->LocalServices.push_back(std::make_pair(
        TActorId(),
        TActorSetupCmd(CreateLabelsMaintainer(Config.GetMonitoringConfig()),
                       TMailboxType::ReadAsFilled, 0)));

    setup->LocalServices.emplace_back(NTestShard::MakeStateServerInterfaceActorId(), TActorSetupCmd(
        NTestShard::CreateStateServerInterfaceActor(nullptr), TMailboxType::ReadAsFilled, 0));

    NKesus::AddKesusProbesList();
}

} // namespace NKikimr::NKikimrServicesInitializers
