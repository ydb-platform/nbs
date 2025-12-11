#include "deletion_markers.h"

#include <cloud/filestore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/fast.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define CHECK_DELETION_MARKERS(n, m, nodeId)                              \
    {                                                                     \
        auto blocksExpected = GenerateBlocks(n, nodeId);                  \
        auto blocksActual = GenerateBlocks(n, nodeId);                    \
        auto [expectedUpdateCount, actualUpdateCount] =                   \
            m.Apply(blocksExpected, blocksActual);                        \
        ASSERT_VECTORS_EQUAL(blocksExpected, blocksActual);               \
        UNIT_ASSERT_VALUES_EQUAL(expectedUpdateCount, actualUpdateCount); \
    }                                                                     \
    // CHECK_DELETION_MARKERS

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 DefaultNodeId = 1;

////////////////////////////////////////////////////////////////////////////////

TVector<TBlock> GenerateBlocks(ui32 n, ui32 nodeId)
{
    TVector<TBlock> blocks{n};
    for (ui32 idx = 0; idx < n; ++idx) {
        blocks[idx].NodeId = nodeId;
        blocks[idx].BlockIndex = idx;
        blocks[idx].MaxCommitId = InvalidCommitId;
    }
    return blocks;
}

////////////////////////////////////////////////////////////////////////////////

struct TReferenceImplementation
{
    TMap<ui64, TVector<ui64>> DeletionMarkersByNodeId;
    TVector<TDeletionMarker> DeletionMarkers;

    void Add(TDeletionMarker deletionMarker)
    {
        DeletionMarkers.push_back(deletionMarker);

        const ui32 begin = deletionMarker.BlockIndex;
        const ui32 end = deletionMarker.BlockIndex + deletionMarker.BlockCount;

        auto& deletionMarkers = DeletionMarkersByNodeId[deletionMarker.NodeId];

        if (end > deletionMarkers.size()) {
            deletionMarkers.resize(end, InvalidCommitId);
        }

        for (ui32 idx = begin; idx < end; ++idx) {
            Y_ABORT_UNLESS(
                deletionMarkers[idx] == InvalidCommitId ||
                deletionMarkers[idx] < deletionMarker.CommitId);

            deletionMarkers[idx] = deletionMarker.CommitId;
        }
    }

    ui32 Apply(TArrayRef<TBlock> blocks) const
    {
        ui32 updateCount = 0;

        for (auto& block: blocks) {
            auto it = DeletionMarkersByNodeId.find(block.NodeId);
            if (it == DeletionMarkersByNodeId.end()) {
                continue;
            }

            auto& deletionMarkers = it->second;

            if (block.BlockIndex >= deletionMarkers.size()) {
                continue;
            }

            const ui64 marker = deletionMarkers[block.BlockIndex];

            if (marker > block.MinCommitId && marker < block.MaxCommitId) {
                block.MaxCommitId = marker;
                ++updateCount;
            }
        }

        return updateCount;
    }

    TVector<TDeletionMarker> Extract()
    {
        DeletionMarkersByNodeId.clear();

        auto extracted = std::move(DeletionMarkers);

        DeletionMarkers = {};

        return extracted;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestDeletionMarkers
{
private:
    TReferenceImplementation Expected;
    TDeletionMarkers Actual;

public:
    TTestDeletionMarkers()
        : Actual(TDefaultAllocator::Instance())
    {}

    void Add(TDeletionMarker deletionMarker)
    {
        Expected.Add(deletionMarker);
        Actual.Add(deletionMarker);
    }

    std::pair<ui32, ui32> Apply(
        TVector<TBlock>& blocksExpected,
        TVector<TBlock>& blocksActual)
    {
        ui32 expectedUpdateCount = Expected.Apply(MakeArrayRef(blocksExpected));
        ui32 actualUpdateCount = Actual.Apply(MakeArrayRef(blocksActual));

        return {expectedUpdateCount, actualUpdateCount};
    }

    std::pair<TVector<TDeletionMarker>, TVector<TDeletionMarker>> Extract()
    {
        auto expectedExtracted = Expected.Extract();
        auto actualExtracted = Actual.Extract();

        return {std::move(expectedExtracted), std::move(actualExtracted)};
    }
};

}   //  namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDeletionMarkersTest)
{
    Y_UNIT_TEST(ShouldWork)
    {
        TTestDeletionMarkers m;

        // lo == map.end()
        // hi == map.end()
        // lo == hi
        m.Add({DefaultNodeId, 1, 10, 10});
        //     10             20
        //     [-------1-------)
        CHECK_DELETION_MARKERS(25, m, DefaultNodeId);

        // lo != map.end() && lo->Start < start
        // hi == map.end()
        // lo != hi
        m.Add({DefaultNodeId, 2, 15, 10});
        //     10     15     20     25
        //     [-------1------)
        //            [-------2-------)
        CHECK_DELETION_MARKERS(25, m, DefaultNodeId);

        // lo != map.end() && lo->Start < start
        // hi != map.end() && hi->Start < end
        // lo == hi
        m.Add({DefaultNodeId, 3, 17, 3});
        //     10     15  17  20    25
        //     [---1--)
        //            [--------2-----)
        //                [--3--)
        CHECK_DELETION_MARKERS(25, m, DefaultNodeId);

        // lo != map.end() && lo->Start >= start
        // hi != map.end() && hi->Start >= end
        // lo != hi
        m.Add({DefaultNodeId, 4, 15, 5});
        //     10     15  17   20    25
        //     [---1--)
        //            [-2-)     [--2--)
        //                [--3--)
        //            [----4----)
        CHECK_DELETION_MARKERS(25, m, DefaultNodeId);

        // lo != map.end() && lo->Start >= start
        // hi != map.end() && hi->Start < end
        // lo != hi
        m.Add({DefaultNodeId, 5, 5, 12});
        // 5     10     15  17   20    25
        //       [---1--)
        //                        [--2--)
        //              [----4----)
        // [--------5-------)
        CHECK_DELETION_MARKERS(25, m, DefaultNodeId);
    }

    Y_UNIT_TEST(ShouldWorkWithMultipleNodeIds)
    {
        TTestDeletionMarkers m;

        m.Add({1, 1, 10, 10});
        m.Add({2, 2, 15, 10});

        CHECK_DELETION_MARKERS(25, m, 1);
        CHECK_DELETION_MARKERS(25, m, 2);
    }

    Y_UNIT_TEST(ShouldExtractDeletionMarkers)
    {
        TTestDeletionMarkers m;

        m.Add({1, 1, 10, 10});
        m.Add({1, 2, 15, 10});

        CHECK_DELETION_MARKERS(25, m, 1);

        auto [expectedExtracted, actualExtracted] = m.Extract();
        ASSERT_VECTORS_EQUAL(expectedExtracted, actualExtracted);

        m.Add({1, 3, 5, 10});

        CHECK_DELETION_MARKERS(25, m, 1);
    }

    Y_UNIT_TEST(ShouldWorkWithRandomlyDistributedDeletionMarkers)
    {
        constexpr ui32 maxN = 1024;
        ui64 commitId = 1;
        TFastRng<ui32> gen(12345);

        TTestDeletionMarkers m;

        for (ui32 step = 0; step < 32 * 1024; ++step) {
            ui32 left = gen.GenRand64() % maxN;
            ui32 right = gen.GenRand64() % maxN;
            if (left > right) {
                std::swap(left, right);
            }

            m.Add({DefaultNodeId, commitId++, left, right - left + 1});
            CHECK_DELETION_MARKERS(maxN, m, DefaultNodeId);
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage

template <>
inline void Out<NCloud::NFileStore::NStorage::TDeletionMarker>(
    IOutputStream& out,
    const NCloud::NFileStore::NStorage::TDeletionMarker& marker)
{
    out << "[" << marker.NodeId << ":" << marker.CommitId << ":"
        << marker.BlockIndex << ":" << marker.BlockCount << "]";
}

template <>
inline void Out<NCloud::NFileStore::NStorage::TBlock>(
    IOutputStream& out,
    const NCloud::NFileStore::NStorage::TBlock& block)
{
    out << "[" << block.NodeId << ":" << block.BlockIndex << ":"
        << block.MinCommitId << ":" << block.MaxCommitId << "]";
}
