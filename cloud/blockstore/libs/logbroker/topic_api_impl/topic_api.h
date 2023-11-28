#pragma once

#include <cloud/blockstore/libs/logbroker/iface/public.h>

#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NBlockStore::NLogbroker {

////////////////////////////////////////////////////////////////////////////////

IServicePtr CreateTopicAPIService(
    TLogbrokerConfigPtr config,
    ILoggingServicePtr logging);

}   // namespace NCloud::NBlockStore::NLogbroker
