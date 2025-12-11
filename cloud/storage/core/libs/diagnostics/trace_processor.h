#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/startable.h>

#include <util/generic/function.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NLWTrace {
class TManager;
}   // namespace NLWTrace

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct ITraceProcessor: public IStartable
{
    virtual ~ITraceProcessor() = default;

    virtual void ForEach(std::function<void(ITraceReaderPtr)> fn) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ITraceProcessorPtr CreateTraceProcessor(
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    TString componentName,
    NLWTrace::TManager& lwManager,
    TVector<ITraceReaderPtr> keepers);

ITraceProcessorPtr CreateTraceProcessorStub();

}   // namespace NCloud
