#include "tablet_tx_rescheduler.h"

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
    const float Probability = 0;
    bool Triggered = false;

public:
    explicit TRandomTxRescheduler(float probabilityPercentage)
        : Probability(probabilityPercentage / 100.0F)
    {}

    bool ShouldReschedule() override
    {
        const bool ret = RandomNumber<float>() < Probability;
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
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITxReschedulerPtr CreateRescheduler(float probabilityPercentage)
{
    return std::make_shared<TRandomTxRescheduler>(probabilityPercentage);
}

}   // namespace NCloud::NFileStore::NStorage
