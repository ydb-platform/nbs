#include "log_chain.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

TLogRecordPtr MakeRecord(ui64 prevLsn, ui64 lsn)
{
    auto record = std::make_shared<TLogRecord>();
    record->PrevLsn = prevLsn;
    record->Lsn = lsn;
    record->Promise = NewPromise<NProto::TError>();
    return record;
}

// Inserts a new record. The chain must take it.
TLogRecordPtr Insert(TLogRecordChain& chain, ui64 prevLsn, ui64 lsn)
{
    auto record = MakeRecord(prevLsn, lsn);
    auto result = chain.Insert(record);
    UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetError().GetCode());
    UNIT_ASSERT(result.GetResult());
    return record;
}

// Inserts a record and marks it as ready, like a finished write does.
TLogRecordPtr InsertReady(TLogRecordChain& chain, ui64 prevLsn, ui64 lsn)
{
    auto record = Insert(chain, prevLsn, lsn);
    UNIT_ASSERT(chain.MarkAsReady(prevLsn));
    return record;
}

// Inserts a ready record, chains it and answers its writer, like the journal
// does. The record must continue the chain.
TLogRecordPtr InsertChained(TLogRecordChain& chain, ui64 prevLsn, ui64 lsn)
{
    auto record = InsertReady(chain, prevLsn, lsn);
    auto chained = chain.ChainNextRecord(prevLsn);
    UNIT_ASSERT_VALUES_EQUAL(record.get(), chained.get());
    record->Promise.SetValue(MakeError(S_OK));
    return record;
}

// Inserts a record that repeats a held one. The chain must not take it and
// must not return an error.
TLogRecordPtr InsertDuplicate(TLogRecordChain& chain, ui64 prevLsn, ui64 lsn)
{
    auto record = MakeRecord(prevLsn, lsn);
    auto result = chain.Insert(record);
    UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetError().GetCode());
    UNIT_ASSERT(!result.GetResult());
    return record;
}

// Returns the error code of an insert that the chain must reject.
ui32 InsertRejected(TLogRecordChain& chain, ui64 prevLsn, ui64 lsn)
{
    auto result = chain.Insert(MakeRecord(prevLsn, lsn));
    UNIT_ASSERT(!result.GetResult());
    return result.GetError().GetCode();
}

bool IsAnswered(const TLogRecordPtr& record)
{
    return record->Promise.HasValue();
}

// Returns the code that the writer of the record has got.
ui32 GetAnswer(const TLogRecordPtr& record)
{
    UNIT_ASSERT(IsAnswered(record));
    return record->Promise.GetFuture().GetValue().GetCode();
}

TVector<ui64> GetLsns(const TVector<TLogRecordPtr>& records)
{
    TVector<ui64> lsns;
    for (const auto& record: records) {
        lsns.push_back(record->Lsn);
    }
    return lsns;
}

TVector<ui64> GetTailLsns(
    const TLogRecordChain& chain,
    ui64 afterLsn,
    ui32 maxRecordCount = 0)
{
    return GetLsns(chain.GetTail(afterLsn, maxRecordCount).first);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLogRecordChainTest)
{
    Y_UNIT_TEST(ShouldRejectRecordWithPrevLsnNotBelowLsn)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, InsertRejected(chain, 25, 20));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, InsertRejected(chain, 20, 20));

        UNIT_ASSERT(!chain.MarkAsReady(25));
        UNIT_ASSERT(!chain.MarkAsReady(20));
    }

    Y_UNIT_TEST(ShouldChainRecordsInOrder)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        auto first = InsertReady(chain, 10, 20);
        auto second = InsertReady(chain, 20, 30);

        UNIT_ASSERT_VALUES_EQUAL(first.get(), chain.ChainNextRecord(10).get());
        UNIT_ASSERT_VALUES_EQUAL(second.get(), chain.ChainNextRecord(20).get());
        UNIT_ASSERT(!chain.ChainNextRecord(30));

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30}),
            GetTailLsns(chain, 10));
    }

    Y_UNIT_TEST(ShouldChainRecordsInsertedOutOfOrder)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        InsertReady(chain, 30, 40);
        InsertReady(chain, 20, 30);

        //
        // Check that nothing continues the chain before the gap is filled
        //

        UNIT_ASSERT(!chain.ChainNextRecord(10));
        UNIT_ASSERT(GetTailLsns(chain, 0).empty());

        InsertReady(chain, 10, 20);

        UNIT_ASSERT_VALUES_EQUAL(20, chain.ChainNextRecord(10)->Lsn);
        UNIT_ASSERT_VALUES_EQUAL(30, chain.ChainNextRecord(20)->Lsn);
        UNIT_ASSERT_VALUES_EQUAL(40, chain.ChainNextRecord(30)->Lsn);

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30, 40}),
            GetTailLsns(chain, 10));
    }

    Y_UNIT_TEST(ShouldNotChainRecordThatIsNotReady)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        Insert(chain, 10, 20);
        InsertReady(chain, 20, 30);

        UNIT_ASSERT(!chain.ChainNextRecord(10));
        UNIT_ASSERT(GetTailLsns(chain, 0).empty());

        UNIT_ASSERT(chain.MarkAsReady(10));

        UNIT_ASSERT_VALUES_EQUAL(20, chain.ChainNextRecord(10)->Lsn);
        UNIT_ASSERT_VALUES_EQUAL(30, chain.ChainNextRecord(20)->Lsn);
    }

    Y_UNIT_TEST(ShouldChainOnlyFromLastChainedLsn)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        InsertReady(chain, 10, 20);
        InsertReady(chain, 20, 30);

        //
        // Check that the chain gives no record for an lsn ahead of its end
        //

        UNIT_ASSERT(!chain.ChainNextRecord(20));

        UNIT_ASSERT_VALUES_EQUAL(20, chain.ChainNextRecord(10)->Lsn);

        //
        // Check that the chain gives no record for an old lsn: another
        // thread has chained from it already
        //

        UNIT_ASSERT(!chain.ChainNextRecord(10));

        UNIT_ASSERT_VALUES_EQUAL(30, chain.ChainNextRecord(20)->Lsn);
    }

    Y_UNIT_TEST(ShouldNotChainAcrossGap)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        InsertReady(chain, 10, 20);
        InsertReady(chain, 30, 40);

        UNIT_ASSERT_VALUES_EQUAL(20, chain.ChainNextRecord(10)->Lsn);
        UNIT_ASSERT(!chain.ChainNextRecord(20));
        UNIT_ASSERT(!chain.ChainNextRecord(30));

        UNIT_ASSERT_VALUES_EQUAL((TVector<ui64>{20}), GetTailLsns(chain, 10));
    }

    Y_UNIT_TEST(ShouldMarkRecordAsReadyByPrevLsn)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        UNIT_ASSERT(!chain.MarkAsReady(10));

        Insert(chain, 10, 20);

        UNIT_ASSERT(chain.MarkAsReady(10));
        UNIT_ASSERT(chain.MarkAsReady(10));

        UNIT_ASSERT_VALUES_EQUAL(20, chain.ChainNextRecord(10)->Lsn);

        //
        // Check that a chained record is not in the pool any more
        //

        UNIT_ASSERT(!chain.MarkAsReady(10));
    }

    Y_UNIT_TEST(ShouldRemoveRecordThatIsNotReady)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        Insert(chain, 10, 20);
        InsertReady(chain, 20, 30);

        UNIT_ASSERT(chain.Remove(10));
        UNIT_ASSERT(!chain.Remove(10));
        UNIT_ASSERT(!chain.Remove(999));
        UNIT_ASSERT(!chain.MarkAsReady(10));

        //
        // Check that a ready record stays: its write is done
        //

        UNIT_ASSERT(!chain.Remove(20));
        UNIT_ASSERT(chain.MarkAsReady(20));
    }

    Y_UNIT_TEST(ShouldTakeRecordAgainAfterRemove)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        InsertChained(chain, 10, 20);
        Insert(chain, 20, 30);
        InsertReady(chain, 30, 40);

        UNIT_ASSERT(chain.Remove(20));
        UNIT_ASSERT(!chain.ChainNextRecord(20));

        InsertReady(chain, 20, 30);

        UNIT_ASSERT_VALUES_EQUAL(30, chain.ChainNextRecord(20)->Lsn);
        UNIT_ASSERT_VALUES_EQUAL(40, chain.ChainNextRecord(30)->Lsn);
    }

    Y_UNIT_TEST(ShouldAnswerDuplicateOfPooledRecord)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        auto record = Insert(chain, 20, 30);
        auto duplicate = InsertDuplicate(chain, 20, 30);

        UNIT_ASSERT(!IsAnswered(duplicate));

        record->Promise.SetValue(MakeError(S_OK));

        UNIT_ASSERT_VALUES_EQUAL(S_OK, GetAnswer(duplicate));
    }

    Y_UNIT_TEST(ShouldAnswerDuplicateOfFailedRecordWithItsError)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        auto record = Insert(chain, 10, 20);
        auto duplicate = InsertDuplicate(chain, 10, 20);

        UNIT_ASSERT(chain.Remove(10));
        record->Promise.SetValue(MakeError(E_IO));

        UNIT_ASSERT_VALUES_EQUAL(E_IO, GetAnswer(duplicate));
    }

    Y_UNIT_TEST(ShouldAnswerDuplicateOfLastChainedRecordAfterItsWriter)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        auto record = InsertReady(chain, 10, 20);
        UNIT_ASSERT_VALUES_EQUAL(record.get(), chain.ChainNextRecord(10).get());

        //
        // Check that the duplicate waits: the record is chained, but its
        // writer has no answer yet, so the record may be not indexed
        //

        auto duplicate = InsertDuplicate(chain, 10, 20);
        UNIT_ASSERT(!IsAnswered(duplicate));

        record->Promise.SetValue(MakeError(S_OK));

        UNIT_ASSERT_VALUES_EQUAL(S_OK, GetAnswer(duplicate));

        //
        // Check that a later duplicate gets the answer at once
        //

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            GetAnswer(InsertDuplicate(chain, 10, 20)));
    }

    Y_UNIT_TEST(ShouldAnswerDuplicateOfChainedRecord)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        InsertChained(chain, 10, 20);
        InsertChained(chain, 20, 30);
        InsertChained(chain, 30, 40);

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            GetAnswer(InsertDuplicate(chain, 10, 20)));
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            GetAnswer(InsertDuplicate(chain, 20, 30)));

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30, 40}),
            GetTailLsns(chain, 10));
    }

    Y_UNIT_TEST(ShouldAnswerAlreadyForRecordAtInitLsn)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(30);

        //
        // Check the record that ends at the init lsn: the chain does not
        // hold it, but the lsn says that it was written before
        //

        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            GetAnswer(InsertDuplicate(chain, 20, 30)));

        UNIT_ASSERT(!chain.MarkAsReady(20));
    }

    Y_UNIT_TEST(ShouldRejectRecordBelowInitLsn)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(30);

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            InsertRejected(chain, 10, 20));
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            InsertRejected(chain, 20, 40));

        InsertChained(chain, 30, 40);
    }

    Y_UNIT_TEST(ShouldRejectExtractedRecord)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        InsertChained(chain, 10, 20);
        InsertChained(chain, 20, 30);

        chain.ExtractRecords(30);

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            InsertRejected(chain, 10, 20));

        //
        // Check that the last chained record is still answered: the chain
        // keeps it after the extract
        //

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            GetAnswer(InsertDuplicate(chain, 20, 30)));
    }

    Y_UNIT_TEST(ShouldRejectForkFromSameLsn)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        auto record = InsertReady(chain, 10, 20);

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            InsertRejected(chain, 10, 25));

        UNIT_ASSERT_VALUES_EQUAL(record.get(), chain.ChainNextRecord(10).get());
    }

    Y_UNIT_TEST(ShouldRejectRecordStartingInsideChain)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        InsertChained(chain, 10, 20);
        InsertChained(chain, 20, 30);

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            InsertRejected(chain, 15, 25));
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            InsertRejected(chain, 15, 35));
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            InsertRejected(chain, 25, 35));
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            InsertRejected(chain, 5, 15));

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30}),
            GetTailLsns(chain, 10));
    }

    Y_UNIT_TEST(ShouldNotDetectOverlapPastGap)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        InsertReady(chain, 30, 40);

        //
        // Check that the chain takes a record that overlaps a pooled one:
        // it finds pooled records only by prev lsn
        //

        Insert(chain, 35, 45);
    }

    Y_UNIT_TEST(ShouldRemoveStrandedRecords)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        auto ready = InsertReady(chain, 15, 25);
        Insert(chain, 18, 28);
        InsertReady(chain, 20, 30);

        UNIT_ASSERT(chain.RemoveStrandedRecords().empty());

        InsertChained(chain, 10, 20);

        //
        // Check that the ready record that starts below the end of the
        // chain is removed: it can never join the chain
        //

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{25}),
            GetLsns(chain.RemoveStrandedRecords()));

        // The chain does not answer the writer, the caller does
        UNIT_ASSERT(!IsAnswered(ready));

        //
        // Check that the record that is not ready stays until its write
        // finishes
        //

        UNIT_ASSERT(chain.RemoveStrandedRecords().empty());
        UNIT_ASSERT(chain.MarkAsReady(18));
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{28}),
            GetLsns(chain.RemoveStrandedRecords()));
        UNIT_ASSERT(!chain.MarkAsReady(18));

        //
        // Check that the record that continues the chain is not touched
        //

        UNIT_ASSERT_VALUES_EQUAL(30, chain.ChainNextRecord(20)->Lsn);
    }

    Y_UNIT_TEST(ShouldInitLsnLowWatermark)
    {
        TLogRecordChain chain;
        UNIT_ASSERT_VALUES_EQUAL(0, chain.GetLsnLowWatermark());

        chain.InitLastChainedLsn(10);
        UNIT_ASSERT_VALUES_EQUAL(10, chain.GetLsnLowWatermark());
    }

    Y_UNIT_TEST(ShouldExtractRecordsUpToLsnLowWatermark)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        InsertChained(chain, 10, 20);
        InsertChained(chain, 20, 30);
        InsertChained(chain, 30, 40);

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30}),
            GetLsns(chain.ExtractRecords(30)));
        UNIT_ASSERT_VALUES_EQUAL(30, chain.GetLsnLowWatermark());
        UNIT_ASSERT_VALUES_EQUAL((TVector<ui64>{40}), GetTailLsns(chain, 0));

        UNIT_ASSERT(chain.ExtractRecords(30).empty());

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{40}),
            GetLsns(chain.ExtractRecords(40)));
        UNIT_ASSERT_VALUES_EQUAL(40, chain.GetLsnLowWatermark());
        UNIT_ASSERT(GetTailLsns(chain, 0).empty());
    }

    Y_UNIT_TEST(ShouldExtractOnlyWholeRecords)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        InsertChained(chain, 10, 20);
        InsertChained(chain, 20, 30);

        UNIT_ASSERT(chain.ExtractRecords(15).empty());
        UNIT_ASSERT_VALUES_EQUAL(15, chain.GetLsnLowWatermark());

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20}),
            GetLsns(chain.ExtractRecords(25)));
        UNIT_ASSERT_VALUES_EQUAL(25, chain.GetLsnLowWatermark());
        UNIT_ASSERT_VALUES_EQUAL((TVector<ui64>{30}), GetTailLsns(chain, 0));
    }

    Y_UNIT_TEST(ShouldChainAfterExtractingAllRecords)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        InsertChained(chain, 10, 20);

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20}),
            GetLsns(chain.ExtractRecords(20)));

        InsertChained(chain, 20, 30);

        UNIT_ASSERT_VALUES_EQUAL((TVector<ui64>{30}), GetTailLsns(chain, 0));
    }

    Y_UNIT_TEST(ShouldReturnEmptyTailWhenEmpty)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        UNIT_ASSERT(GetTailLsns(chain, 0).empty());
        UNIT_ASSERT(GetTailLsns(chain, 10).empty());
        UNIT_ASSERT(GetTailLsns(chain, 100).empty());
    }

    Y_UNIT_TEST(ShouldReturnTailAfterLsn)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        InsertChained(chain, 10, 20);
        InsertChained(chain, 20, 30);
        InsertChained(chain, 30, 40);

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30, 40}),
            GetTailLsns(chain, 0));
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30, 40}),
            GetTailLsns(chain, 10));
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{30, 40}),
            GetTailLsns(chain, 20));
        UNIT_ASSERT_VALUES_EQUAL((TVector<ui64>{40}), GetTailLsns(chain, 30));

        UNIT_ASSERT(GetTailLsns(chain, 40).empty());
        UNIT_ASSERT(GetTailLsns(chain, 100).empty());
    }

    Y_UNIT_TEST(ShouldReturnTailAfterLsnInsideRecord)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        InsertChained(chain, 10, 20);
        InsertChained(chain, 20, 30);

        //
        // Check that the tail starts from the first record above the lsn,
        // also when the lsn is not the end of a record
        //

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30}),
            GetTailLsns(chain, 15));
        UNIT_ASSERT_VALUES_EQUAL((TVector<ui64>{30}), GetTailLsns(chain, 25));
    }

    Y_UNIT_TEST(ShouldLimitTailByMaxRecordCount)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        InsertChained(chain, 10, 20);
        InsertChained(chain, 20, 30);
        InsertChained(chain, 30, 40);

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20}),
            GetTailLsns(chain, 10, 1));
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30}),
            GetTailLsns(chain, 10, 2));
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{30}),
            GetTailLsns(chain, 20, 1));
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30, 40}),
            GetTailLsns(chain, 10, 100));
    }

    Y_UNIT_TEST(ShouldTreatZeroMaxRecordCountAsNoLimit)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        InsertChained(chain, 10, 20);
        InsertChained(chain, 20, 30);
        InsertChained(chain, 30, 40);

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30, 40}),
            GetTailLsns(chain, 10, 0));
        UNIT_ASSERT(GetTailLsns(chain, 40, 0).empty());
    }

    Y_UNIT_TEST(ShouldNotReturnPooledRecordsInTail)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        InsertChained(chain, 10, 20);
        InsertReady(chain, 20, 30);
        Insert(chain, 30, 40);

        UNIT_ASSERT_VALUES_EQUAL((TVector<ui64>{20}), GetTailLsns(chain, 10));
    }

    Y_UNIT_TEST(ShouldReturnLsnLowWatermarkWithTail)
    {
        TLogRecordChain chain;
        chain.InitLastChainedLsn(10);

        InsertChained(chain, 10, 20);
        InsertChained(chain, 20, 30);

        UNIT_ASSERT_VALUES_EQUAL(10, chain.GetTail(0, 0).second);

        chain.ExtractRecords(20);

        auto [records, lsnLowWatermark] = chain.GetTail(0, 0);
        UNIT_ASSERT_VALUES_EQUAL((TVector<ui64>{30}), GetLsns(records));
        UNIT_ASSERT_VALUES_EQUAL(20, lsnLowWatermark);
    }
}

}   // namespace NCloud::NJournalled
