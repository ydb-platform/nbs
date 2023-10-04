#pragma once

#include "public.h"

#include <cloud/filestore/libs/storage/tablet/model/public.h>
#include <cloud/filestore/libs/storage/tablet/protos/tablet.pb.h>

#include <util/generic/hash.h>
#include <util/generic/intrlist.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TCheckpoint
    : public TIntrusiveListItem<TCheckpoint>
    , public NProto::TCheckpoint
{
public:
    TCheckpoint(const NProto::TCheckpoint& proto)
        : NProto::TCheckpoint(proto)
    {}
};

using TCheckpointList = TIntrusiveListWithAutoDelete<TCheckpoint, TDelete>;
using TCheckpointMap = THashMap<TString, TCheckpoint*>;

////////////////////////////////////////////////////////////////////////////////

class TCheckpointStore
{
private:
    TCheckpointList Checkpoints;
    TCheckpointMap CheckpointById;

    TSet<ui64> CommitIds;

public:
    TCheckpoint* CreateCheckpoint(const NProto::TCheckpoint& proto);

    void MarkCheckpointDeleted(TCheckpoint* checkpoint);
    void RemoveCheckpoint(TCheckpoint* checkpoint);

    TVector<TCheckpoint*> GetCheckpoints() const;

    TCheckpoint* FindCheckpoint(const TString& checkpointId) const;
    TCheckpoint* FindCheckpoint(ui64 checkpointId) const;

    ui64 FindCheckpoint(ui64 nodeId, ui64 commitId) const;
};

}   // namespace NCloud::NFileStore::NStorage
