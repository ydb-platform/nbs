#pragma once

#include "public.h"

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct IIncompleteRequestProcessor
{
    virtual ~IIncompleteRequestProcessor() = default;

    virtual void UpdateStats(bool updateIntervalFinished) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IIncompleteRequestProcessorPtr CreateInflightRequestCollectorStub();

}   // namespace NCloud
