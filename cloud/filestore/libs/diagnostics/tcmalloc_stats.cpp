#include "tcmalloc_stats.h"

#include <cloud/storage/core/libs/diagnostics/stats_handler.h>

#include <contrib/ydb/core/mon_alloc/tcmalloc.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTcMallocStatsHandler final: public IStatsHandler
{
private:
    std::unique_ptr<NKikimr::IAllocStats> AllocStats;

public:
    explicit TTcMallocStatsHandler(
        NMonitoring::TDynamicCountersPtr rootCounters)
        : AllocStats(
              NKikimr::CreateTcMallocStats(
                  rootCounters->GetSubgroup("counters", "utils")))
    {}

    void UpdateStats(bool updateIntervalFinished) override
    {
        Y_UNUSED(updateIntervalFinished);

        AllocStats->Update();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStatsHandlerPtr CreateTcMallocStatsHandler(
    NMonitoring::TDynamicCountersPtr rootCounters)
{
    return std::make_shared<TTcMallocStatsHandler>(std::move(rootCounters));
}

}   // namespace NCloud::NFileStore
