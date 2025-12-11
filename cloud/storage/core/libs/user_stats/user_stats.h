#pragma once

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/api/public.h>

namespace NCloud::NStorage::NUserStats {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateStorageUserStats(
    int component,
    TString path,
    TString title,
    TVector<IUserMetricsSupplierPtr> providers);

}   // namespace NCloud::NStorage::NUserStats
