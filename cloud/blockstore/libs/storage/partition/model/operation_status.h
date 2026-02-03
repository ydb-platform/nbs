#pragma once

#include <library/cpp/protobuf/json/proto2json.h>

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

NJson::TJsonValue ToJson(const NPartition::TOperationState& op);

void DumpOperationState(
    IOutputStream& out,
    const NPartition::TOperationState& op);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
