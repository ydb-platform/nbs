#include "fresh_bytes.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFreshBytesVisitor final
    : public IFreshBytesVisitor
{
    TVector<TBytes> Bytes;
    TString Data;

    bool InsertBreaks = true;

    void Accept(const TBytes& bytes, TStringBuf data) override
    {
        Bytes.push_back(bytes);
        if (InsertBreaks && Data) {
            Data += "|";
        }
        Data += data;
    }
};

////////////////////////////////////////////////////////////////////////////////

TString GenerateData(ui32 len)
{
    TString data(len, 0);
    for (ui32 i = 0; i < len; ++i) {
        data[i] = 'a' + (i % ('z' - 'a' + 1));
    }
    return data;
}

////////////////////////////////////////////////////////////////////////////////

#define COMPARE_BYTES(expected, actual)                                        \
    for (ui32 i = 0; i < Max(expected.size(), actual.size()); ++i) {           \
        TString actualStr = "(none)";                                          \
        if (i < actual.size()) {                                               \
            actualStr = actual[i].Describe();                                  \
        }                                                                      \
        TString expectedStr = "(none)";                                        \
        if (i < expectedStr.size()) {                                          \
            expectedStr = expected[i].Describe();                              \
        }                                                                      \
        UNIT_ASSERT_VALUES_EQUAL(expectedStr, actualStr);                      \
    }                                                                          \
// COMPARE_BYTES

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFreshBytesTest)
{
    Y_UNIT_TEST(ShouldStoreBytes)
    {
        TFreshBytes freshBytes(TDefaultAllocator::Instance());

        freshBytes.AddBytes(1, 100, "aAa", 10);
        freshBytes.AddBytes(1, 101, "bBbB", 11);
        freshBytes.AddBytes(1, 50, "cCc", 12);
        freshBytes.AddBytes(1, 50, "dDd", 13);
        freshBytes.AddBytes(2, 100, "eEeEe", 14);
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            freshBytes.CheckBytes(2, 1000, "fFf", 15).GetCode());
        freshBytes.AddBytes(2, 1000, "fFf", 15);
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            freshBytes.CheckBytes(2, 1000, "zzz", 9).GetCode());
        freshBytes.AddDeletionMarker(2, 100, 3, 16);

        {
            TFreshBytesVisitor visitor;
            freshBytes.FindBytes(visitor, 1, TByteRange(0, 1000, 4_KB), 15);

            COMPARE_BYTES(
                TVector<TBytes>({
                    {1, 50, 3, 13, InvalidCommitId},
                    {1, 100, 1, 10, InvalidCommitId},
                    {1, 101, 4, 11, InvalidCommitId},
                }), visitor.Bytes);

            UNIT_ASSERT_VALUES_EQUAL("dDd|a|bBbB", visitor.Data);
        }

        {
            TFreshBytesVisitor visitor;
            freshBytes.FindBytes(visitor, 2, TByteRange(0, 1000, 4_KB), 15);

            COMPARE_BYTES(
                TVector<TBytes>({
                    {2, 103, 2, 14, InvalidCommitId},
                }), visitor.Bytes);

            UNIT_ASSERT_VALUES_EQUAL("Ee", visitor.Data);
        }

        TVector<TBytes> bytes;
        TVector<TBytes> deletionMarkers;
        auto info = freshBytes.StartCleanup(17, &bytes, &deletionMarkers);

        COMPARE_BYTES(
            TVector<TBytes>({
                {1, 100, 3, 10, InvalidCommitId},
                {1, 101, 4, 11, InvalidCommitId},
                {1, 50, 3, 12, InvalidCommitId},
                {1, 50, 3, 13, InvalidCommitId},
                {2, 100, 5, 14, InvalidCommitId},
                {2, 1000, 3, 15, InvalidCommitId},
            }), bytes);
        COMPARE_BYTES(
            TVector<TBytes>({
                {2, 100, 3, 16, InvalidCommitId},
            }), deletionMarkers);
        UNIT_ASSERT_VALUES_EQUAL(17, info.ClosingCommitId);

        TVector<TBytes> visitedBytes;
        TVector<TBytes> visitedDeletionMarkers;
        auto visitTop = [&] () {
            visitedBytes.clear();
            visitedDeletionMarkers.clear();
            constexpr ui64 itemLimit = 100;
            freshBytes.VisitTop(
                itemLimit,
                [&] (const TBytes& bytes, bool isDel) {
                    if (isDel) {
                        visitedDeletionMarkers.push_back(bytes);
                    } else {
                        visitedBytes.push_back(bytes);
                    }
                }
            );
        };

        visitTop();
        COMPARE_BYTES(bytes, visitedBytes);
        COMPARE_BYTES(deletionMarkers, visitedDeletionMarkers);

        UNIT_ASSERT(freshBytes.FinishCleanup(
            info.ChunkId,
            visitedBytes.size(),
            visitedDeletionMarkers.size()));

        {
            TFreshBytesVisitor visitor;
            freshBytes.FindBytes(visitor, 1, TByteRange(0, 1000, 4_KB), 14);
            COMPARE_BYTES(TVector<TBytes>(), visitor.Bytes);
            UNIT_ASSERT_VALUES_EQUAL(TString(), visitor.Data);
        }

        {
            TFreshBytesVisitor visitor;
            freshBytes.FindBytes(visitor, 2, TByteRange(0, 1000, 4_KB), 14);
            COMPARE_BYTES(TVector<TBytes>(), visitor.Bytes);
            UNIT_ASSERT_VALUES_EQUAL(TString(), visitor.Data);
        }

        freshBytes.AddDeletionMarker(2, 100, 1024, 18);
        freshBytes.AddDeletionMarker(3, 1000, 200, 19);

        bytes.clear();
        deletionMarkers.clear();
        info = freshBytes.StartCleanup(19, &bytes, &deletionMarkers);

        COMPARE_BYTES(TVector<TBytes>(), bytes);
        COMPARE_BYTES(
            TVector<TBytes>({
                {2, 100, 1024, 18, InvalidCommitId},
                {3, 1000, 200, 19, InvalidCommitId},
            }), deletionMarkers);
        UNIT_ASSERT_VALUES_EQUAL(19, info.ClosingCommitId);

        visitTop();
        COMPARE_BYTES(bytes, visitedBytes);
        COMPARE_BYTES(deletionMarkers, visitedDeletionMarkers);

        UNIT_ASSERT(freshBytes.FinishCleanup(
            info.ChunkId,
            visitedBytes.size(),
            visitedDeletionMarkers.size()));
    }

    Y_UNIT_TEST(ShouldApplyDeletionMarkersToPreviousChunks)
    {
        TFreshBytes freshBytes(TDefaultAllocator::Instance());

        freshBytes.AddBytes(1, 100, "aAa", 10);
        freshBytes.AddBytes(1, 101, "bBbB", 11);
        freshBytes.AddBytes(1, 50, "cCc", 12);
        freshBytes.AddBytes(1, 50, "dDd", 13);
        freshBytes.AddBytes(2, 100, "eEeEe", 14);
        freshBytes.AddBytes(2, 70, "12345678", 15);
        freshBytes.AddDeletionMarker(2, 100, 3, 16);

        TVector<TBytes> bytes;
        TVector<TBytes> deletionMarkers;
        auto info = freshBytes.StartCleanup(17, &bytes, &deletionMarkers);

        COMPARE_BYTES(
            TVector<TBytes>({
                {1, 100, 3, 10, InvalidCommitId},
                {1, 101, 4, 11, InvalidCommitId},
                {1, 50, 3, 12, InvalidCommitId},
                {1, 50, 3, 13, InvalidCommitId},
                {2, 100, 5, 14, InvalidCommitId},
                {2, 70, 8, 15, InvalidCommitId},
            }), bytes);
        COMPARE_BYTES(
            TVector<TBytes>({
                {2, 100, 3, 16, InvalidCommitId},
            }), deletionMarkers);
        UNIT_ASSERT_VALUES_EQUAL(17, info.ClosingCommitId);

        freshBytes.AddBytes(1, 106, "qwertyuiop", 18);
        freshBytes.AddDeletionMarker(1, 102, 9, 19);

        {
            TFreshBytesVisitor visitor;
            freshBytes.FindBytes(visitor, 1, TByteRange(0, 1000, 4_KB), 19);

            COMPARE_BYTES(
                TVector<TBytes>({
                    {1, 50, 3, 13, InvalidCommitId},
                    {1, 100, 1, 10, InvalidCommitId},
                    {1, 101, 1, 11, InvalidCommitId},
                    {1, 111, 5, 18, InvalidCommitId},
                }), visitor.Bytes);

            UNIT_ASSERT_VALUES_EQUAL("dDd|a|b|yuiop", visitor.Data);
        }

        {
            TFreshBytesVisitor visitor;
            freshBytes.FindBytes(visitor, 2, TByteRange(0, 1000, 4_KB), 19);

            COMPARE_BYTES(
                TVector<TBytes>({
                    {2, 70, 8, 15, InvalidCommitId},
                    {2, 103, 2, 14, InvalidCommitId},
                }), visitor.Bytes);

            UNIT_ASSERT_VALUES_EQUAL("12345678|Ee", visitor.Data);
        }

        TVector<TBytes> visitedBytes;
        TVector<TBytes> visitedDeletionMarkers;
        auto visitTop = [&] () {
            visitedBytes.clear();
            visitedDeletionMarkers.clear();
            constexpr ui64 itemLimit = 100;
            freshBytes.VisitTop(
                itemLimit,
                [&] (const TBytes& bytes, bool isDel) {
                    if (isDel) {
                        visitedDeletionMarkers.push_back(bytes);
                    } else {
                        visitedBytes.push_back(bytes);
                    }
                }
            );
        };

        visitTop();
        COMPARE_BYTES(bytes, visitedBytes);
        COMPARE_BYTES(deletionMarkers, visitedDeletionMarkers);

        UNIT_ASSERT(freshBytes.FinishCleanup(
            info.ChunkId,
            visitedBytes.size(),
            visitedDeletionMarkers.size()));

        {
            TFreshBytesVisitor visitor;
            freshBytes.FindBytes(visitor, 1, TByteRange(0, 1000, 4_KB), 19);

            COMPARE_BYTES(
                TVector<TBytes>({
                    {1, 111, 5, 18, InvalidCommitId},
                }), visitor.Bytes);

            UNIT_ASSERT_VALUES_EQUAL("yuiop", visitor.Data);
        }

        {
            TFreshBytesVisitor visitor;
            freshBytes.FindBytes(visitor, 2, TByteRange(0, 1000, 4_KB), 19);

            COMPARE_BYTES(
                TVector<TBytes>({
                }), visitor.Bytes);

            UNIT_ASSERT_VALUES_EQUAL("", visitor.Data);
        }

        bytes.clear();
        deletionMarkers.clear();
        info = freshBytes.StartCleanup(20, &bytes, &deletionMarkers);
        visitTop();
        COMPARE_BYTES(bytes, visitedBytes);
        COMPARE_BYTES(deletionMarkers, visitedDeletionMarkers);
    }

    Y_UNIT_TEST(ShouldInsertIntervalInTheMiddleOfAnotherInterval)
    {
        TFreshBytes freshBytes(TDefaultAllocator::Instance());

        TString data = GenerateData(1663);

        freshBytes.AddBytes(1, 0, data, 10);
        freshBytes.AddBytes(1, 4, "1234", 11);

        {
            TFreshBytesVisitor visitor;
            visitor.InsertBreaks = false;
            freshBytes.FindBytes(visitor, 1, TByteRange(0, 4_KB, 4_KB), 11);

            COMPARE_BYTES(
                TVector<TBytes>({
                    {1, 0, 4, 10, InvalidCommitId},
                    {1, 4, 4, 11, InvalidCommitId},
                    {1, 8, 1655, 10, InvalidCommitId},
                }), visitor.Bytes);

            TString expected = data;
            expected[4] = '1';
            expected[5] = '2';
            expected[6] = '3';
            expected[7] = '4';

            UNIT_ASSERT_VALUES_EQUAL(expected, visitor.Data);
        }
    }

    Y_UNIT_TEST(ShouldNotOverflowInTBytesLength)
    {
        constexpr ui32 dataSize = 64_KB;

        TFreshBytes freshBytes(TDefaultAllocator::Instance());

        ui32 commitId = 1;
        TString data = GenerateData(dataSize);

        freshBytes.AddBytes(465, 0, TStringBuf(data.data(), dataSize), commitId);

        TVector<TBytes> entries;
        TVector<TBytes> deletionMarkers;
        freshBytes.StartCleanup(commitId, &entries, &deletionMarkers);

        for (auto& entry: entries) {
            UNIT_ASSERT_VALUES_EQUAL(entry.Length, dataSize);
        }
    }

    Y_UNIT_TEST(ShouldObeyLimitProvidedForVisitTop)
    {
        TFreshBytes freshBytes(TDefaultAllocator::Instance());

        freshBytes.AddBytes(1, 100, "aAa", 10);
        freshBytes.AddBytes(1, 101, "bBbB", 11);
        freshBytes.AddBytes(1, 50, "cCc", 12);
        freshBytes.AddBytes(1, 50, "dDd", 13);
        freshBytes.AddBytes(2, 100, "eEeEe", 14);
        freshBytes.AddBytes(2, 1000, "fFf", 15);
        freshBytes.AddDeletionMarker(2, 100, 3, 16);

        TVector<TBytes> bytes;
        TVector<TBytes> deletionMarkers;
        auto info = freshBytes.StartCleanup(17, &bytes, &deletionMarkers);

        COMPARE_BYTES(
            TVector<TBytes>({
                {1, 100, 3, 10, InvalidCommitId},
                {1, 101, 4, 11, InvalidCommitId},
                {1, 50, 3, 12, InvalidCommitId},
                {1, 50, 3, 13, InvalidCommitId},
                {2, 100, 5, 14, InvalidCommitId},
                {2, 1000, 3, 15, InvalidCommitId},
            }), bytes);
        COMPARE_BYTES(
            TVector<TBytes>({
                {2, 100, 3, 16, InvalidCommitId},
            }), deletionMarkers);
        UNIT_ASSERT_VALUES_EQUAL(17, info.ClosingCommitId);

        TVector<TBytes> visitedBytes;
        TVector<TBytes> visitedDeletionMarkers;

        constexpr ui64 itemsLimit = 2;

        auto visitTop = [&] () {
            visitedBytes.clear();
            visitedDeletionMarkers.clear();
            freshBytes.VisitTop(
                itemsLimit,
                [&] (const TBytes& bytes, bool isDel) {
                    if (isDel) {
                        visitedDeletionMarkers.push_back(bytes);
                    } else {
                        visitedBytes.push_back(bytes);
                    }
                }
            );
        };

        {
            TVector<TBytes> expectedBytes {
                {1, 100, 3, 10, InvalidCommitId},
                {1, 101, 4, 11, InvalidCommitId},
            };
            TVector<TBytes> expectedDeletionMarkers;

            visitTop();
            COMPARE_BYTES(expectedBytes, visitedBytes);
            COMPARE_BYTES(expectedDeletionMarkers, visitedDeletionMarkers);

            UNIT_ASSERT(!freshBytes.FinishCleanup(
                info.ChunkId,
                visitedBytes.size(),
                visitedDeletionMarkers.size()));
        }

        {
            TVector<TBytes> expectedBytes {
                {1, 50, 3, 12, InvalidCommitId},
                {1, 50, 3, 13, InvalidCommitId},
            };
            TVector<TBytes> expectedDeletionMarkers;

            visitTop();
            COMPARE_BYTES(expectedBytes, visitedBytes);
            COMPARE_BYTES(expectedDeletionMarkers, visitedDeletionMarkers);

            UNIT_ASSERT(!freshBytes.FinishCleanup(
                info.ChunkId,
                visitedBytes.size(),
                visitedDeletionMarkers.size()));
        }

        {
            TVector<TBytes> expectedBytes {
                {2, 100, 5, 14, InvalidCommitId},
                {2, 1000, 3, 15, InvalidCommitId},
            };
            TVector<TBytes> expectedDeletionMarkers;

            visitTop();
            COMPARE_BYTES(expectedBytes, visitedBytes);
            COMPARE_BYTES(expectedDeletionMarkers, visitedDeletionMarkers);

            UNIT_ASSERT(!freshBytes.FinishCleanup(
                info.ChunkId,
                visitedBytes.size(),
                visitedDeletionMarkers.size()));
        }

        {
            TVector<TBytes> expectedBytes;
            TVector<TBytes> expectedDeletionMarkers {
                {2, 100, 3, 16, InvalidCommitId},
            };

            visitTop();
            COMPARE_BYTES(expectedBytes, visitedBytes);
            COMPARE_BYTES(expectedDeletionMarkers, visitedDeletionMarkers);

            UNIT_ASSERT(freshBytes.FinishCleanup(
                info.ChunkId,
                visitedBytes.size(),
                visitedDeletionMarkers.size()));
        }

        {
            TFreshBytesVisitor visitor;
            freshBytes.FindBytes(visitor, 1, TByteRange(0, 1000, 4_KB), 14);
            COMPARE_BYTES(TVector<TBytes>(), visitor.Bytes);
            UNIT_ASSERT_VALUES_EQUAL(TString(), visitor.Data);
        }
    }

    // TODO test all branches of AddBytes

    // TODO test with multiple chunks
}

namespace {

////////////////////////////////////////////////////////////////////////////////

TString DescribeRanges(const TVector<TByteRange>& ranges)
{
    TStringBuilder sb;
    for (const auto& r: ranges) {
        if (sb.size()) {
            sb << " ";
        }
        sb << r.Describe();
    }
    return sb;
}

TByteRange R(ui64 offset, ui64 length)
{
    return TByteRange(offset, length, 4_KB);
}

TString D(std::initializer_list<TByteRange> ranges)
{
    return DescribeRanges(TVector<TByteRange>(ranges));
}

TString Apply(
    const TDeletedRangeMap& map,
    ui64 nodeId,
    ui64 offset,
    ui64 length,
    ui64 commitId)
{
    return DescribeRanges(map.ApplyRanges(nodeId, R(offset, length), commitId));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TDeletedRangeMapTest)
{
    Y_UNIT_TEST(ShouldReturnFullRangeWhenEmpty)
    {
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        UNIT_ASSERT_VALUES_EQUAL(D({R(0, 100)}), Apply(map, 1, 0, 100, 10));
    }

    Y_UNIT_TEST(ShouldReturnEmptyWhenEmptyMapAndZeroLengthQuery)
    {
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        UNIT_ASSERT_VALUES_EQUAL(D({}), Apply(map, 1, 10, 0, 10));
    }

    Y_UNIT_TEST(ShouldSubtractSingleDeletionFromMiddle)
    {
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        map.AddRange(1, 30, 20, 5);
        UNIT_ASSERT_VALUES_EQUAL(
            D({R(0, 30), R(50, 50)}),
            Apply(map, 1, 0, 100, 10));
    }

    Y_UNIT_TEST(ShouldReturnEmptyWhenQueryEqualsDeletion)
    {
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        map.AddRange(1, 30, 20, 5);
        UNIT_ASSERT_VALUES_EQUAL(D({}), Apply(map, 1, 30, 20, 10));
    }

    Y_UNIT_TEST(ShouldReturnEmptyWhenQueryFullyInsideDeletion)
    {
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        map.AddRange(1, 0, 100, 5);
        UNIT_ASSERT_VALUES_EQUAL(D({}), Apply(map, 1, 25, 50, 10));
    }

    Y_UNIT_TEST(ShouldTrimQueryStartWhenDeletionCoversStart)
    {
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        // deletion [10, 30) intersects query [15, 40) at the start.
        map.AddRange(1, 10, 20, 5);
        UNIT_ASSERT_VALUES_EQUAL(D({R(30, 10)}), Apply(map, 1, 15, 25, 10));
    }

    Y_UNIT_TEST(ShouldTrimQueryEndWhenDeletionCoversEnd)
    {
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        // deletion [30, 60) intersects query [0, 40) at the end.
        map.AddRange(1, 30, 30, 5);
        UNIT_ASSERT_VALUES_EQUAL(D({R(0, 30)}), Apply(map, 1, 0, 40, 10));
    }

    Y_UNIT_TEST(ShouldIgnoreDeletionOutsideOfQuery)
    {
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        map.AddRange(1, 200, 50, 5);
        UNIT_ASSERT_VALUES_EQUAL(D({R(0, 100)}), Apply(map, 1, 0, 100, 10));
    }

    Y_UNIT_TEST(ShouldIgnoreDeletionEndingAtQueryStart)
    {
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        // deletion [0, 50) ends exactly at query.Offset (50). No overlap.
        map.AddRange(1, 0, 50, 5);
        UNIT_ASSERT_VALUES_EQUAL(
            D({R(50, 50)}),
            Apply(map, 1, 50, 50, 10));
    }

    Y_UNIT_TEST(ShouldIgnoreDeletionStartingAtQueryEnd)
    {
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        // deletion [50, 100) starts exactly at query.End (50). No overlap.
        map.AddRange(1, 50, 50, 5);
        UNIT_ASSERT_VALUES_EQUAL(D({R(0, 50)}), Apply(map, 1, 0, 50, 10));
    }

    Y_UNIT_TEST(ShouldNotApplyDeletionWhenCommitIdTooLow)
    {
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        map.AddRange(1, 30, 20, 20);
        // At commit 10 the deletion (commit 20) is not yet visible.
        UNIT_ASSERT_VALUES_EQUAL(
            D({R(0, 100)}),
            Apply(map, 1, 0, 100, 10));
    }

    Y_UNIT_TEST(ShouldApplyDeletionWhenCommitIdEqualsQueryCommitId)
    {
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        map.AddRange(1, 30, 20, 10);
        UNIT_ASSERT_VALUES_EQUAL(
            D({R(0, 30), R(50, 50)}),
            Apply(map, 1, 0, 100, 10));
    }

    Y_UNIT_TEST(ShouldSubtractMultipleNonOverlappingDeletions)
    {
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        map.AddRange(1, 20, 10, 5);
        map.AddRange(1, 50, 10, 6);
        map.AddRange(1, 80, 10, 7);
        UNIT_ASSERT_VALUES_EQUAL(
            D({R(0, 20), R(30, 20), R(60, 20), R(90, 10)}),
            Apply(map, 1, 0, 100, 10));
    }

    Y_UNIT_TEST(ShouldPreserveAdjacentDeletions)
    {
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        // Adjacent (touching) ranges are stored as distinct entries and both
        // apply on read.
        map.AddRange(1, 0, 10, 5);
        map.AddRange(1, 10, 10, 6);
        UNIT_ASSERT_VALUES_EQUAL(D({}), Apply(map, 1, 0, 20, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            D({R(20, 10)}),
            Apply(map, 1, 0, 30, 10));
    }

    Y_UNIT_TEST(ShouldCoalesceOutputAcrossInvisibleDeletion)
    {
        // Deletions [0, 10)@5 and [20, 30)@15. Reading at commit 10 makes
        // the second deletion invisible, and ApplyRanges does not split the
        // output around invisible entries: the tail is returned as a single
        // range spanning past the invisible deletion.
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        map.AddRange(1, 0, 10, 5);
        map.AddRange(1, 20, 10, 15);
        UNIT_ASSERT_VALUES_EQUAL(
            D({R(10, 30)}),
            Apply(map, 1, 0, 40, 10));
        // Reading at commit 15 makes both visible and the output is split.
        UNIT_ASSERT_VALUES_EQUAL(
            D({R(10, 10), R(30, 10)}),
            Apply(map, 1, 0, 40, 15));
    }

    Y_UNIT_TEST(ShouldKeepOlderCommitIdOnOverlappingAdd)
    {
        // First deletion [0, 10)@5. Then AddRange [5, 15)@6. The overlap
        // [5, 10) keeps commit 5 (older wins on overlap); only [10, 15)
        // gets commit 6.
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        map.AddRange(1, 0, 10, 5);
        map.AddRange(1, 5, 10, 6);

        // At commit 5, only [0, 10) is visible.
        UNIT_ASSERT_VALUES_EQUAL(
            D({R(10, 10)}),
            Apply(map, 1, 0, 20, 5));

        // At commit 6, [10, 15) is now visible too.
        UNIT_ASSERT_VALUES_EQUAL(
            D({R(15, 5)}),
            Apply(map, 1, 0, 20, 6));
    }

    Y_UNIT_TEST(ShouldSpanMultipleExistingRangesOnAdd)
    {
        // Preexisting deletions [10, 20)@5 and [30, 40)@6.
        // Add [0, 100)@7: gaps [0,10), [20,30), [40,100) get commit 7.
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        map.AddRange(1, 10, 10, 5);
        map.AddRange(1, 30, 10, 6);
        map.AddRange(1, 0, 100, 7);

        // At commit 4 nothing is visible.
        UNIT_ASSERT_VALUES_EQUAL(
            D({R(0, 100)}),
            Apply(map, 1, 0, 100, 4));

        // At commit 5 only [10, 20) is visible.
        UNIT_ASSERT_VALUES_EQUAL(
            D({R(0, 10), R(20, 80)}),
            Apply(map, 1, 0, 100, 5));

        // At commit 6 [10, 20) and [30, 40) are visible.
        UNIT_ASSERT_VALUES_EQUAL(
            D({R(0, 10), R(20, 10), R(40, 60)}),
            Apply(map, 1, 0, 100, 6));

        // At commit 7 everything is deleted.
        UNIT_ASSERT_VALUES_EQUAL(D({}), Apply(map, 1, 0, 100, 7));
    }

    Y_UNIT_TEST(ShouldNotSplitExistingRangeWhenAddIsFullyContained)
    {
        // AddRange fully inside a lower-commit deletion is absorbed: the
        // pre-existing deletion still covers everything, so the new commitId
        // never becomes observable within the overlap.
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        map.AddRange(1, 0, 100, 5);
        map.AddRange(1, 30, 20, 10);

        // At commit 5 the whole [0, 100) is deleted.
        UNIT_ASSERT_VALUES_EQUAL(
            D({}),
            Apply(map, 1, 0, 100, 5));

        // At commit 4 nothing is deleted.
        UNIT_ASSERT_VALUES_EQUAL(
            D({R(0, 100)}),
            Apply(map, 1, 0, 100, 4));

        // At commit 9 (between 5 and 10) still the [0, 100) deletion applies.
        UNIT_ASSERT_VALUES_EQUAL(
            D({}),
            Apply(map, 1, 0, 100, 9));
    }

    Y_UNIT_TEST(ShouldSplitExistingRangeWhenAddCoversWithBiggerRange)
    {
        // Preexisting [30, 50)@5. AddRange [0, 100)@10 becomes
        // [0, 30)@10, [30, 50)@5 (preserved), [50, 100)@10.
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        map.AddRange(1, 30, 20, 5);
        map.AddRange(1, 0, 100, 10);

        // At commit 7 only [30, 50)@5 is visible.
        UNIT_ASSERT_VALUES_EQUAL(
            D({R(0, 30), R(50, 50)}),
            Apply(map, 1, 0, 100, 7));

        // At commit 10 all three cover the whole range.
        UNIT_ASSERT_VALUES_EQUAL(D({}), Apply(map, 1, 0, 100, 10));
    }

    Y_UNIT_TEST(ShouldIsolateDifferentNodeIds)
    {
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        map.AddRange(1, 0, 100, 5);
        map.AddRange(2, 50, 20, 5);

        // NodeId 1 has [0, 100) deleted.
        UNIT_ASSERT_VALUES_EQUAL(D({}), Apply(map, 1, 0, 100, 10));

        // NodeId 2 only has [50, 70) deleted.
        UNIT_ASSERT_VALUES_EQUAL(
            D({R(0, 50), R(70, 30)}),
            Apply(map, 2, 0, 100, 10));

        // NodeId 3 has no deletions.
        UNIT_ASSERT_VALUES_EQUAL(
            D({R(0, 100)}),
            Apply(map, 3, 0, 100, 10));
    }

    Y_UNIT_TEST(ShouldNotConsiderDeletionsFromOtherNodeIdWhenSpanning)
    {
        // A pre-existing deletion on node 1 must not be touched or spanned
        // when AddRange is called for node 2.
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        map.AddRange(1, 30, 20, 5);
        map.AddRange(2, 0, 100, 10);

        // Node 2 has [0, 100) deleted.
        UNIT_ASSERT_VALUES_EQUAL(D({}), Apply(map, 2, 0, 100, 10));

        // Node 1 keeps its original [30, 50) deletion.
        UNIT_ASSERT_VALUES_EQUAL(
            D({R(0, 30), R(50, 50)}),
            Apply(map, 1, 0, 100, 10));
    }

    Y_UNIT_TEST(ShouldRespectQueryOffsetAfterMap)
    {
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        map.AddRange(1, 0, 100, 5);
        // Query that starts past every deletion returns the query unchanged.
        UNIT_ASSERT_VALUES_EQUAL(
            D({R(200, 50)}),
            Apply(map, 1, 200, 50, 10));
    }

    Y_UNIT_TEST(ShouldRespectQueryEndBeforeMap)
    {
        TDeletedRangeMap map(TDefaultAllocator::Instance());
        map.AddRange(1, 200, 50, 5);
        UNIT_ASSERT_VALUES_EQUAL(
            D({R(0, 100)}),
            Apply(map, 1, 0, 100, 10));
    }
}

}   // namespace NCloud::NFileStore::NStorage
