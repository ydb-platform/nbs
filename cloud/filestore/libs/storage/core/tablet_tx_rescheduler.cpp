#include "tablet_tx_rescheduler.h"

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
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITxReschedulerPtr CreateNoOpTxRescheduler()
{
    return std::make_shared<TNoOpTxRescheduler>();
}

}   // namespace NCloud::NFileStore::NStorage
