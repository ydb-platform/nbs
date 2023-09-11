#include "tenant.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/mind/local.h>
#include <ydb/core/tx/coordinator/coordinator.h>
#include <ydb/core/tx/mediator/mediator.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>

namespace NCloud::NStorage {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void ConfigureTenantSystemTablets(const TAppData& appData, TLocalConfig& localConfig)
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
}

}   // namespace NCloud::NStorage
