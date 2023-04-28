#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IPostponeTimePredictor
{
    virtual ~IPostponeTimePredictor() = default;

    virtual void Register(TDuration postponeDelay) = 0;
    virtual TDuration GetPossiblePostponeDuration() = 0;
};

////////////////////////////////////////////////////////////////////////////////

IPostponeTimePredictorPtr CreatePostponeTimePredictor(
    ITimerPtr timer,
    TDuration delayWindowInterval,
    double delayWindowPercentage,
    TMaybe<TDuration> delayUpperBound);
IPostponeTimePredictorPtr CreatePostponeTimePredictorStub();

}   // namespace NCloud::NBlockStore
