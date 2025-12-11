#include "incomplete_request_processor.h"

#include <memory>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TIncompleteRequestProcessorStub: public IIncompleteRequestProcessor
{
    void UpdateStats(bool updateIntervalFinished) override
    {
        Y_UNUSED(updateIntervalFinished);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IIncompleteRequestProcessorPtr CreateIncompleteRequestProcessorStub()
{
    return std::make_shared<TIncompleteRequestProcessorStub>();
}

}   // namespace NCloud
