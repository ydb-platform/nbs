#include "stats_handler.h"

#include <memory>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TStatsHandlerStub
    : public IStatsHandler
{
    void UpdateStats(bool updateIntervalFinished) override
    {
        Y_UNUSED(updateIntervalFinished);
    }
};

}  // namespace

////////////////////////////////////////////////////////////////////////////////

IStatsHandlerPtr CreateStatsHandlerStub()
{
    return std::make_shared<TStatsHandlerStub>();
}

}   // namespace NCloud
