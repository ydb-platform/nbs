#pragma once

#include <util/system/types.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Decides whether a tablet transaction should be rescheduled (restarted) from
// PrepareTx. Tests inject an implementation that reschedules to make the
// transaction restart/reorder path.
struct ITxRescheduler
{
    virtual ~ITxRescheduler() = default;

    virtual bool ShouldReschedule() = 0;
    virtual bool IsTriggered() const = 0;
    virtual void Reset() = 0;
};

using ITxReschedulerPtr = std::shared_ptr<ITxRescheduler>;

////////////////////////////////////////////////////////////////////////////////

// Creates a rescheduler that forces a read transaction to be restarted
// with a given probability.
ITxReschedulerPtr CreateRescheduler(float probabilityPercentage);

}   // namespace NCloud::NFileStore::NStorage
