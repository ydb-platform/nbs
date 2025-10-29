#pragma once

#include "public.h"

#include <ydb/core/driver_lib/run/kikimr_services_initializers.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

using namespace NKikimr;

using namespace NCloud::NStorage;

class TKikimrServicesInitializer final
    : public IServiceInitializer
{
private:
    const NKikimrConfig::TAppConfigPtr Config;

public:
    explicit TKikimrServicesInitializer(NKikimrConfig::TAppConfigPtr config)
        : Config{std::move(config)}
    {}

    void InitializeServices(
        TActorSystemSetup* setup,
        const TAppData* appData) override;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NStorage
