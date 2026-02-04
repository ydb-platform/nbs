#include "operation_status.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NJson;

////////////////////////////////////////////////////////////////////////////////

NJson::TJsonValue ToJson(const NPartition::TOperationState& op)
{
    TJsonValue json;
    json["Status"] = ToString(op.Status);
    const auto duration = TInstant::Now() - op.Timestamp;
    json["Duration"] = duration.MicroSeconds();
    return json;
}

void DumpOperationState(
    IOutputStream& out,
    const NPartition::TOperationState& op)
{
    out << ToString(op.Status);

    if (op.Timestamp != TInstant::Zero()) {
        out << " for " << TInstant::Now() - op.Timestamp;
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
