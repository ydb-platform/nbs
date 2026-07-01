#pragma once

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Decides whether a tablet transaction should be rescheduled (restarted) from
// PrepareTx. Production uses a no-op stub; tests inject an implementation that
// reschedules to make the transaction restart/reorder path.
struct ITxRescheduler
{
    virtual ~ITxRescheduler() = default;

    virtual bool ShouldReschedule() = 0;
};

using ITxReschedulerPtr = std::shared_ptr<ITxRescheduler>;

////////////////////////////////////////////////////////////////////////////////

// Default rescheduler used in production: never reschedules.
ITxReschedulerPtr CreateNoOpTxRescheduler();

}   // namespace NCloud::NFileStore::NStorage
