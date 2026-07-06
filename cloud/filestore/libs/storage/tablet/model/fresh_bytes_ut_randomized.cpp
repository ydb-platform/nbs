#include "fresh_bytes.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/random/fast.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TNodeData
{
    TString Content;
    ui64 CommitId = 0;

    bool operator<(const TNodeData& rhs) const
    {
        return CommitId < rhs.CommitId;
    }

    bool operator<(ui64 commitId) const
    {
        return CommitId < commitId;
    }
};

struct TNodeState
{
    ui64 NodeId = 0;
    TVector<TNodeData> Data;
    TVector<TByteRange> Ranges;

    void AddBytes(ui64 offset, TStringBuf data, ui64 commitId)
    {
        TString last;
        if (Data.size()) {
            last = Data.back().Content;
            UNIT_ASSERT_GT(commitId, Data.back().CommitId);
        }

        if (last.size() < offset + data.size()) {
            last.resize(offset + data.size(), 0);
        }

        memcpy(last.begin() + offset, data.data(), data.size());

        Data.push_back({std::move(last), commitId});
        Ranges.push_back(TByteRange(offset, data.size(), 4_KB));
    }

    void AddDeletionMarker(ui64 offset, ui64 len, ui64 commitId)
    {
        if (Data.empty()) {
            return;
        }

        auto last = Data.back().Content;
        if (offset >= last.size()) {
            return;
        }

        if (offset + len >= last.size()) {
            last.resize(offset);
            while (last.size() && last.back() == 0) {
                last.resize(last.size() - 1);
            }
        } else {
            memset(last.begin() + offset, 0, len);
        }

        Data.push_back({std::move(last), commitId});
    }

    void FindBytes(
        IFreshBytesVisitor& visitor,
        TByteRange byteRange,
        ui64 commitId) const
    {
        const auto* it = std::lower_bound(Data.begin(), Data.end(), commitId);
        if (it == Data.end() || it->CommitId > commitId) {
            if (it == Data.begin()) {
                return;
            }

            --it;
        }

        TByteRange dataRange(0, it->Content.size(), 4_KB);
        const auto intersection = dataRange.Intersect(byteRange);
        if (!intersection.Length) {
            return;
        }

        Cdbg << "N=" << NodeId << " CRANGE: " << dataRange.Describe()
            << " " << it->CommitId << Endl;
        auto content = TStringBuf(it->Content).substr(
            intersection.Offset,
            intersection.Offset + intersection.Length);
        TBytes bytes(
            NodeId,
            intersection.Offset,
            intersection.Length,
            it->CommitId,
            InvalidCommitId);
        visitor.Accept(bytes, content);
    }

    [[nodiscard]] bool Intersects(TByteRange byteRange) const
    {
        for (const auto& range: Ranges) {
            if (range.Overlaps(byteRange)) {
                return true;
            }
        }

        return false;
    }
};

struct TCheckpointState
{
    ui64 ChunkId = 0;
    ui64 DataItemCount = 0;
    ui64 DeletionMarkerCount = 0;

    THashMap<ui64, TNodeState> NodeId2State;

    void AddBytes(ui64 nodeId, ui64 offset, TStringBuf data, ui64 commitId)
    {
        AccessNode(nodeId).AddBytes(offset, data, commitId);
    }

    void AddDeletionMarker(ui64 nodeId, ui64 offset, ui64 len, ui64 commitId)
    {
        AccessNode(nodeId).AddDeletionMarker(offset, len, commitId);
    }

    void FindBytes(
        IFreshBytesVisitor& visitor,
        ui64 nodeId,
        TByteRange byteRange,
        ui64 commitId) const
    {
        if (const auto* p = NodeId2State.FindPtr(nodeId)) {
            p->FindBytes(visitor, byteRange, commitId);
        }
    }

    [[nodiscard]] bool Intersects(ui64 nodeId, TByteRange byteRange) const
    {
        if (const auto* p = NodeId2State.FindPtr(nodeId)) {
            return p->Intersects(byteRange);
        }

        return false;
    }

    TNodeState& AccessNode(ui64 nodeId)
    {
        auto& node = NodeId2State[nodeId];
        node.NodeId = nodeId;
        return node;
    }
};

struct TReferenceImplementation
{
    TMap<ui64, TCheckpointState> CommitId2Checkpoint;
    TCheckpointState Current;
    ui64 LastChunkId = 1;
    TDeque<ui64> CleanupCommitIds;

    TReferenceImplementation()
    {
        Current.ChunkId = LastChunkId;
    }

    [[nodiscard]] NProto::TError CheckBytes(
        ui64 nodeId,
        ui64 offset,
        TStringBuf data,
        ui64 commitId) const
    {
        Y_UNUSED(nodeId, offset, data, commitId);

        return MakeError(E_NOT_IMPLEMENTED);
    }

    void AddBytes(ui64 nodeId, ui64 offset, TStringBuf data, ui64 commitId)
    {
        Current.AddBytes(nodeId, offset, data, commitId);
    }

    void AddDeletionMarker(ui64 nodeId, ui64 offset, ui64 len, ui64 commitId)
    {
        Current.AddDeletionMarker(nodeId, offset, len, commitId);
    }

    void OnCheckpoint(ui64 commitId)
    {
        if (!Current.DataItemCount && !Current.DeletionMarkerCount) {
            return;
        }

        CommitId2Checkpoint[commitId] = Current;
        Current.ChunkId = ++LastChunkId;
        Current.DataItemCount = 0;
        Current.DeletionMarkerCount = 0;
    }

    TFlushBytesCleanupInfo StartCleanup(
        ui64 commitId,
        TVector<TBytes>* entries,
        TVector<TBytes>* deletionMarkers)
    {
        Y_UNUSED(entries, deletionMarkers);

        TFlushBytesCleanupInfo result;
        if (CommitId2Checkpoint.empty()) {
            result.ChunkId = Current.ChunkId;
            result.ClosingCommitId = commitId;
            CleanupCommitIds.push_back(commitId);
            OnCheckpoint(commitId);
        } else {
            result.ChunkId = CommitId2Checkpoint.begin()->second.ChunkId;
            result.ClosingCommitId = CommitId2Checkpoint.begin()->first;
        }

        return result;
    }

    void VisitTop(ui64 itemLimit, const TChunkVisitor& visitor)
    {
        Y_UNUSED(itemLimit, visitor);
    }

    bool FinishCleanup(
        ui64 chunkId,
        ui64 dataItemCount,
        ui64 deletionMarkerCount)
    {
        Y_UNUSED(dataItemCount, deletionMarkerCount);

        UNIT_ASSERT(CommitId2Checkpoint.size() > 0);
        auto it = CommitId2Checkpoint.begin();
        UNIT_ASSERT_VALUES_EQUAL(it->second.ChunkId, chunkId);
        it->second.DataItemCount -= dataItemCount;
        it->second.DeletionMarkerCount -= deletionMarkerCount;
        if (!it->second.DataItemCount && !it->second.DeletionMarkerCount) {
            CommitId2Checkpoint.erase(it);
            return true;
        }

        return false;
    }

    void FindBytes(
        IFreshBytesVisitor& visitor,
        ui64 nodeId,
        TByteRange byteRange,
        ui64 commitId) const
    {
        auto it = CommitId2Checkpoint.lower_bound(commitId);
        if (it != CommitId2Checkpoint.end()) {
            it->second.FindBytes(visitor, nodeId, byteRange, commitId);
            return;
        }

        Current.FindBytes(visitor, nodeId, byteRange, commitId);
    }

    [[nodiscard]] bool Intersects(ui64 nodeId, TByteRange byteRange) const
    {
        for (const auto& x: CommitId2Checkpoint) {
            if (x.second.Intersects(nodeId, byteRange)) {
                return true;
            }
        }

        return Current.Intersects(nodeId, byteRange);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TFreshBytesVisitor final
    : public IFreshBytesVisitor
{
    TVector<TString> NodeId2Content;

    explicit TFreshBytesVisitor(ui64 nodeCount)
        : NodeId2Content(nodeCount + 1)
    {}

    void Accept(const TBytes& bytes, TStringBuf data) override
    {
        UNIT_ASSERT(NodeId2Content.size() > bytes.NodeId);
        Cdbg << "N=" << bytes.NodeId << " ACCEPT " << bytes.Offset
            << " " << bytes.Length << Endl;

        auto& content = NodeId2Content[bytes.NodeId];
        if (content.Size() < bytes.Offset + bytes.Length) {
            content.resize(bytes.Offset + bytes.Length, 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(bytes.Length, data.size());
        memcpy(content.begin() + bytes.Offset, data.data(), data.size());
    }
};

////////////////////////////////////////////////////////////////////////////////

TString GenerateData(TFastRng64& rng, ui32 len)
{
    TString data(len, 0);
    for (ui32 i = 0; i < len; ++i) {
        data[i] = 'a' + ((i + rng.Uniform(100)) % ('z' - 'a' + 1));
    }
    return data;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRandomizedFreshBytesTest)
{
    Y_UNIT_TEST(ShouldStoreBytes)
    {
        TFreshBytes freshBytes(TDefaultAllocator::Instance());
        TReferenceImplementation ri;

        const ui32 iters = 1000;
        const ui32 nodeCount = 10;
        const ui64 maxOffset = 2_KB;
        const ui32 maxLength = 2_KB;
        const double deletionProb = 0.2;
        const double checkpointProb = 0.05;
        const ui64 seed = 777;

        TFastRng64 rng(seed);
        ui64 currentCommitId = 1;

        TVector<ui64> checkpoints;

        for (ui32 i = 0; i < iters; ++i) {
            const ui64 nodeId = rng.Uniform(0, nodeCount) + 1;
            const ui64 offset = rng.Uniform(0, maxOffset);
            const ui32 len = rng.Uniform(0, maxLength) + 1;

            Cdbg << "ITER=" << i << " N=" << nodeId << " " << offset << " "
                << len << Endl;
            if (rng.GenRandReal2() < deletionProb) {
                freshBytes.AddDeletionMarker(
                    nodeId,
                    offset,
                    len,
                    currentCommitId);
                ri.AddDeletionMarker(nodeId, offset, len, currentCommitId);
                Cdbg << "N=" << nodeId << " MARKER " << offset
                    << " " << currentCommitId << " " << len << Endl;
            } else {
                const auto data = GenerateData(rng, len);
                freshBytes.AddBytes(nodeId, offset, data, currentCommitId);
                ri.AddBytes(nodeId, offset, data, currentCommitId);
                Cdbg << "N=" << nodeId << " DATA " << offset
                    << " " << currentCommitId << " " << data << Endl;
            }

            if (rng.GenRandReal2() < checkpointProb) {
                freshBytes.OnCheckpoint(currentCommitId);
                ri.OnCheckpoint(currentCommitId);
                checkpoints.push_back(currentCommitId);
            }

            ++currentCommitId;
        }

        checkpoints.push_back(InvalidCommitId);

        for (const ui64 commitId: checkpoints) {
            TFreshBytesVisitor visitor(nodeCount);
            for (ui64 i = 0; i < nodeCount; ++i) {
                const ui64 nodeId = i + 1;
                freshBytes.FindBytes(
                    visitor,
                    nodeId,
                    TByteRange(0, maxOffset + maxLength, 4_KB),
                    commitId);
            }

            TFreshBytesVisitor riVisitor(nodeCount);
            for (ui64 i = 0; i < nodeCount; ++i) {
                const ui64 nodeId = i + 1;
                ri.FindBytes(
                    riVisitor,
                    nodeId,
                    TByteRange(0, maxOffset + maxLength, 4_KB),
                    commitId);
            }

            for (ui64 i = 0; i < nodeCount; ++i) {
                const ui64 nodeId = i + 1;
                UNIT_ASSERT_VALUES_EQUAL_C(
                    riVisitor.NodeId2Content[nodeId],
                    visitor.NodeId2Content[nodeId],
                    TStringBuilder() << "nodeId=" << nodeId
                        << ", commitId=" << commitId);
            }
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
