#include "tablet_tx_rescheduler.h"

#include <util/random/random.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TNoOpTxRescheduler final
    : public ITxRescheduler
{
public:
    bool ShouldReschedule() override
    {
        return false;
    }

    bool IsTriggered() const override
    {
        return false;
    }

    void Reset() override
    {
    }
};

// With given probability decides if a transaction should be rescheduled.
// Used in tests to exercise behaviour in case of transaction restarts/reorderings.
// Note that it does not reschedule anything by itself and only serves for communication
// between TIndexActorDatabase and Tx::Execute().
class TRandomTxRescheduler final
    : public ITxRescheduler
{
private:
    const ui32 ProbabilityPct = 0;
    bool Triggered = false;

public:
    explicit TRandomTxRescheduler(ui32 probabilityPct)
        : ProbabilityPct(probabilityPct)
    {}

    bool ShouldReschedule() override
    {
        const bool ret = RandomNumber<ui32>(100) < ProbabilityPct;
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

ITxReschedulerPtr CreateRescheduler(ui32 probabilityPct)
{
    return std::make_shared<TRandomTxRescheduler>(probabilityPct);
}

ITxReschedulerPtr CreateNoOpTxRescheduler()
{
    return std::make_shared<TNoOpTxRescheduler>();
}

}   // namespace NCloud::NFileStore::NStorage
