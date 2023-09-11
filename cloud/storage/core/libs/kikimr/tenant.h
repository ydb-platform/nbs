#pragma once

namespace NKikimr {
    struct TAppData;
    struct TLocalConfig;
};

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

void ConfigureTenantSystemTablets(
    const NKikimr::TAppData& appData,
    NKikimr::TLocalConfig& config);

}   // namespace NCloud::NStorage
