#pragma once

#include <cloud/blockstore/libs/storage/partition_common/model/operation_status.h>

#include <util/datetime/base.h>

#include <initializer_list>

namespace NCloud::NBlockStore::NStorage::NPartition {

// The volume supplies a deadline shared by all partition boot attempts in a
// GC cycle. Standalone partition callers can initialize it at activation.
class TSmallBlobsRemovalState
{
private:
    bool Enabled = false;
    bool Finished = false;
    TInstant Deadline;

public:
    TSmallBlobsRemovalState() = default;

    TSmallBlobsRemovalState(bool enabled, TInstant deadline)
        : Enabled(enabled)
        , Deadline(deadline)
    {}

    void Activate(TInstant now, TDuration timeout)
    {
        if (Enabled && !Deadline) {
            Deadline = now + timeout;
        }
    }

    bool IsEnabled() const
    {
        return Enabled;
    }

    bool IsStopped(TInstant now) const
    {
        return Enabled && (Finished || (Deadline && now >= Deadline));
    }

    bool IsActive(TInstant now) const
    {
        return Enabled && !IsStopped(now);
    }

    bool IsReady(TInstant now,
                 std::initializer_list<EOperationStatus> operations) const
    {
        if (IsStopped(now)) {
            return true;
        }
        for (const auto status: operations) {
            if (status != EOperationStatus::Idle) {
                return false;
            }
        }
        return true;
    }

    void Finish()
    {
        Finished = true;
    }

    void Disable()
    {
        Enabled = false;
        Finished = false;
        Deadline = {};
    }
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
