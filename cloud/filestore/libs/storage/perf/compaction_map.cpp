#include <cloud/filestore/libs/storage/tablet/tablet_state.h>

#include <library/cpp/testing/benchmark/bench.h>

using namespace NCloud;
using namespace NCloud::NFileStore;
using namespace NCloud::NFileStore::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

TVector<TCompactionRangeInfo> GenerateTestData()
{
    TVector<TCompactionRangeInfo> compactionRanges;
    ui32 rangeId = 0;
    for (ui32 i = 1; i <= 1e3; ++i) {
        while (rangeId < TCompactionMap::GroupSize * i) {
            compactionRanges.emplace_back(rangeId, TCompactionStats{100, 100});
            rangeId += 2;
        }
    }
    return compactionRanges;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

const auto CompactionRanges = GenerateTestData();

Y_CPU_BENCHMARK(TCompactionMap_Load, iface)
{
    for (size_t i = 0; i < iface.Iterations(); ++i) {
        TIndexTabletState tabletState;
        tabletState.LoadCompactionMap(CompactionRanges);
    }
}
