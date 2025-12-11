#pragma once

#include "public.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>

#include <array>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class EAuthorizationStatus
{
    PermissionsGranted = 0,
    PermissionsDenied,
    PermissionsGrantedWhenDisabled,
    PermissionsDeniedWhenDisabled,
    PermissionsGrantedWithEmptyToken,
    PermissionsDeniedWithEmptyToken,
    PermissionsDeniedWithoutFolderId,

    MAX
};

////////////////////////////////////////////////////////////////////////////////

class TAuthCounters final: public TAtomicRefCount<TAuthCounters>
{
private:
    std::array<
        NMonitoring::TDynamicCounters::TCounterPtr,
        (int)EAuthorizationStatus::MAX>
        AuthorizationStatusCounters;

public:
    explicit TAuthCounters(
        NMonitoring::TDynamicCounterPtr counters,
        TString counterId);

    void ReportAuthorizationStatus(EAuthorizationStatus status);
};

using TAuthCountersPtr = TIntrusivePtr<TAuthCounters>;

}   // namespace NCloud::NStorage
