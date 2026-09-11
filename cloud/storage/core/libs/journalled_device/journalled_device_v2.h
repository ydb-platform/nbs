#pragma once

#include "public.h"

#include "journalled_device.h"

#include <cloud/storage/core/libs/coroutine/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <util/generic/string.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

IJournalledDevicePtr CreateJournalledDeviceV2(
    ILoggingServicePtr logging,
    TExecutorPtr executor,
    IJournalPtr journal,
    IDevicePtr dataStore,
    TString deviceUUID,
    TString backgroundClientId);

}   // namespace NCloud::NJournalled
