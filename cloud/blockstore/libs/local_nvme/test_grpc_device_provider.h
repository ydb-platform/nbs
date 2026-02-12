#pragma once

#include "public.h"

#include <cloud/storage/core/libs/diagnostics/public.h>

#include <util/generic/fwd.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

auto CreateTestGrpcDeviceProvider(
    ILoggingServicePtr logging,
    TString socketPath) -> ILocalNVMeDeviceProviderPtr;

}   // namespace NCloud::NBlockStore
