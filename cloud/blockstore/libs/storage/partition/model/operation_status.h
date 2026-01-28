#pragma once

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

enum class EOperationStatus
{
    Idle,
    Enqueued,
    Started,
};

////////////////////////////////////////////////////////////////////////////////

struct TOperationState
{
    EOperationStatus Status = EOperationStatus::Idle;
    TInstant Timestamp;

    void SetStatus(EOperationStatus status)
    {
        Status = status;
        Timestamp = TInstant::Now();
    }
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
