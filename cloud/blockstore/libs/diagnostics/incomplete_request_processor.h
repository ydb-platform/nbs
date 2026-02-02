#pragma once

#include "public.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

NCloud::IStatsHandlerPtr CreateIncompleteRequestProcessor(
    IServerStatsPtr stats,
    TVector<IIncompleteRequestProviderPtr> incompleteProviders);

NCloud::IStatsHandlerPtr CreateIncompleteRequestProcessorStub();

}   // namespace NCloud::NBlockStore
