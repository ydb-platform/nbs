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
            freshBytes.VisitTop([&] (const TBytes& bytes, bool isDel) {
                if (isDel) {
                    visitedDeletionMarkers.push_back(bytes);
                } else {
                    visitedBytes.push_back(bytes);
                }
            });
        };

        visitTop();
        COMPARE_BYTES(bytes, visitedBytes);
        COMPARE_BYTES(deletionMarkers, visitedDeletionMarkers);

        freshBytes.FinishCleanup(info.ChunkId);

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

        freshBytes.FinishCleanup(info.ChunkId);
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

    // TODO test all branches of AddBytes

    // TODO test with multiple chunks
}

}   // namespace NCloud::NFileStore::NStorage
