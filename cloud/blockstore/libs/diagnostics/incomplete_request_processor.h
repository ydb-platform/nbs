#pragma once

#include "public.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

IIncompleteRequestProcessorPtr CreateIncompleteRequestProcessor(
    IServerStatsPtr stats,
    TVector<IIncompleteRequestProviderPtr> incompleteProviders);

IIncompleteRequestProcessorPtr CreateIncompleteRequestProcessorStub();

}   // namespace NCloud::NBlockStore
