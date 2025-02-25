#include "tenant.h"

#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/core/base/hive.h>
#include <contrib/ydb/core/mind/local.h>
#include <contrib/ydb/core/statistics/aggregator/aggregator.h>
#include <contrib/ydb/core/sys_view/processor/processor.h>
#include <contrib/ydb/core/tx/coordinator/coordinator.h>
#include <contrib/ydb/core/tx/mediator/mediator.h>
#include <contrib/ydb/core/tx/schemeshard/schemeshard.h>

namespace NCloud::NStorage {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

ui64 GetHiveTabletId(const TActorContext& ctx)
{
    auto& domainsInfo = *AppData(ctx)->DomainsInfo;
    Y_ABORT_UNLESS(domainsInfo.Domains);

    auto domainUid = domainsInfo.Domains.begin()->first;
    auto hiveUid = domainsInfo.GetDefaultHiveUid(domainUid);

    return domainsInfo.GetHive(hiveUid);
}

void ConfigureTenantSystemTablets(
    const TAppData& appData,
    TLocalConfig& localConfig,
    bool allowAdditionalSystemTablets)
{
    localConfig.TabletClassInfo.emplace(
        TTabletTypes::SchemeShard,
        TLocalConfig::TTabletClassInfo(
            MakeIntrusive<TTabletSetupInfo>(
                &CreateFlatTxSchemeShard,
                TMailboxType::ReadAsFilled,
                appData.SystemPoolId,
                TMailboxType::ReadAsFilled,
                appData.SystemPoolId)));

    localConfig.TabletClassInfo.emplace(
        TTabletTypes::Hive,
        TLocalConfig::TTabletClassInfo(
            MakeIntrusive<TTabletSetupInfo>(
                &CreateDefaultHive,
                TMailboxType::ReadAsFilled,
                appData.SystemPoolId,
                TMailboxType::ReadAsFilled,
                appData.SystemPoolId)));

    localConfig.TabletClassInfo.emplace(
        TTabletTypes::Mediator,
        TLocalConfig::TTabletClassInfo(
            MakeIntrusive<TTabletSetupInfo>(
                &CreateTxMediator,
                TMailboxType::Revolving,
                appData.SystemPoolId,
                TMailboxType::Revolving,
                appData.SystemPoolId)));

    localConfig.TabletClassInfo.emplace(
        TTabletTypes::Coordinator,
        TLocalConfig::TTabletClassInfo(
            MakeIntrusive<TTabletSetupInfo>(
                &CreateFlatTxCoordinator,
                TMailboxType::Revolving,
                appData.SystemPoolId,
                TMailboxType::Revolving,
                appData.SystemPoolId)));

    if (allowAdditionalSystemTablets) {
        localConfig.TabletClassInfo.emplace(
            TTabletTypes::SysViewProcessor,
            TLocalConfig::TTabletClassInfo(
                MakeIntrusive<TTabletSetupInfo>(
                    &NSysView::CreateSysViewProcessor,
                    TMailboxType::Revolving,
                    appData.UserPoolId,
                    TMailboxType::Revolving,
                    appData.UserPoolId)));

        localConfig.TabletClassInfo.emplace(
            TTabletTypes::StatisticsAggregator,
            TLocalConfig::TTabletClassInfo(
                MakeIntrusive<TTabletSetupInfo>(
                    &NStat::CreateStatisticsAggregator,
                    TMailboxType::Revolving,
                    appData.UserPoolId,
                    TMailboxType::Revolving,
                    appData.UserPoolId)));
    }
}

}   // namespace NCloud::NStorage
