#include "transaction_time_tracker.h"

#include <library/cpp/testing/gbenchmark/benchmark.h>

namespace NCloud::NBlockStore::NStorage {

///////////////////////////////////////////////////////////////////////////////

static void TransactionTimeTracker(benchmark::State& state)
{
    TTransactionTimeTracker tracker;
    constexpr size_t IoDepth = 1000;

    const auto transactionTypes =
        TTransactionTimeTracker::GetTransactionBuckets();

    TVector<ui64> running;
    ui64 idGenerator = 0;

    for (const auto _: state) {
        const ui64 id = ++idGenerator;
        const size_t transactionType =
            RandomNumber<ui64>(transactionTypes.size());

        tracker.OnStarted(
            id,
            transactionTypes[transactionType].Key,
            GetCycleCount());
        if (running.size() < IoDepth) {
            running.push_back(id);
        } else {
            const size_t finishedIdx = RandomNumber<ui64>(running.size());

            tracker.OnFinished(running[finishedIdx], GetCycleCount());
            running[finishedIdx] = id;
        }
    }
}

BENCHMARK(TransactionTimeTracker);

}   // namespace NCloud::NBlockStore::NStorage
