#pragma once

#include "public.h"

#include "rebase_logic.h"

#include <cloud/blockstore/libs/storage/protos/part.pb.h>

#include <cloud/storage/core/libs/tablet/model/commit.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <library/cpp/json/json_value.h>

#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

struct TCheckpointInfo
{
    TVector<TPartialBlobId> BlobIds;
    ui32 Idx = 0;

    TCheckpointInfo() = default;

    TCheckpointInfo(TVector<TPartialBlobId> blobIds)
        : BlobIds(std::move(blobIds))
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TCheckpointStorage final: public IPivotalCommitStorage
{
    using TCheckpointMap = THashMap<TString, NProto::TCheckpointMeta>;

private:
    TCheckpointMap Checkpoints;
    TVector<ui64> CommitIds;
    ui64 CheckpointBlockCount = 0;

public:
    bool Add(const NProto::TCheckpointMeta& meta);
    bool Remove(const TString& checkpointId);

    const NProto::TCheckpointMeta* Find(const TString& checkpointId) const;
    ui64 GetCommitId(const TString& checkpointId) const;

    ui64 RebaseCommitId(ui64 commitId) const override;

    TVector<NProto::TCheckpointMeta> Get() const;
    const TVector<ui64>& GetCommitIds() const;

    void SetCheckpointBlockCount(ui64 c);
    ui64 GetCheckpointBlockCount() const;

    NJson::TJsonValue AsJson() const;

    ui64 GetCount() const;
    bool IsEmpty() const;

private:
    void InsertCommitId(ui64 commitId);
    bool RemoveCommitId(ui64 commitId);
};

////////////////////////////////////////////////////////////////////////////////

class TCheckpointsToDelete
{
private:
    THashMap<ui64, TCheckpointInfo> CommitId2Info;
    TDeque<ui64> CommitIds;

public:
    void Put(ui64 commitId, TCheckpointInfo info);
    TVector<TPartialBlobId> DeleteNextCheckpoint();
    TVector<TPartialBlobId> ExtractBlobsToCleanup(size_t limit, ui64* commitId);

    bool IsEmpty() const
    {
        return CommitIds.empty();
    }
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
