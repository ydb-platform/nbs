#include "tablet_tx_rescheduler.h"

#include <cloud/filestore/libs/storage/api/components.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <util/random/mersenne.h>
#include <util/random/random.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

// With a given probability decides if a transaction should be rescheduled.
// Used in tests to exercise behaviour in case of transaction restarts/reorderings.
// Note that it does not reschedule anything by itself and only serves for communication
// between TIndexActorDatabase and Tx::Execute().
class TRandomTxRescheduler final
    : public ITxRescheduler
{
private:
    const double Probability = 0;
    bool Triggered = false;
    ui64 Seed;
    TMersenne<ui64> RandomGen;

public:
    explicit TRandomTxRescheduler(float probabilityPercentage,
                                  std::optional<ui64> randomSeed)
        : Probability(probabilityPercentage / 100.0)
        , Seed(randomSeed ? *randomSeed : RandomNumber<ui64>())
        , RandomGen(Seed)
    {
    }

    bool ShouldReschedule() override
    {
        const bool ret = RandomGen.GenRandReal4() < Probability;
        Triggered |= ret;
        return ret;
    }

    bool IsTriggered() const override
    {
        return Triggered;
    }

    void Reset() override
    {
        Triggered = false;
    }

    ui64 GetSeed() const override
    {
        return Seed;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITxReschedulerPtr CreateRescheduler(TReschedulerParams params)
{
    return std::make_shared<TRandomTxRescheduler>(
        params.ProbabilityPercentage,
        params.RandomSeed);
}

}   // namespace NCloud::NFileStore::NStorage
