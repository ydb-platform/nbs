#include "checkpoint.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TCheckpointStore::Add(const TString& checkpointId)
{
    Checkpoints.emplace(checkpointId, false);
}

void TCheckpointStore::DeleteData(const TString& checkpointId)
{
    if (auto* dataDeleted = Checkpoints.FindPtr(checkpointId)) {
        *dataDeleted = true;
    }
}

void TCheckpointStore::Delete(const TString& checkpointId)
{
    Checkpoints.erase(checkpointId);
}

bool TCheckpointStore::Empty() const
{
    return Checkpoints.empty();
}

size_t TCheckpointStore::Size() const
{
    return Checkpoints.size();
}

TVector<TString> TCheckpointStore::Get() const
{
    TVector<TString> checkpoints(Reserve(Checkpoints.size()));
    for (const auto& [checkpoint, dataDeleted]: Checkpoints) {
        if (!dataDeleted) {
            checkpoints.push_back(checkpoint);
        }
    }
    return checkpoints;
}

TString TCheckpointStore::GetLast() const
{
    TVector<TString> checkpoints = Get();
    if (checkpoints.empty()) {
        return {};
    }

    return checkpoints.back();
}

const THashMap<TString, bool>& TCheckpointStore::GetAll() const
{
    return Checkpoints;
}

}   // namespace NCloud::NBlockStore::NStorage
