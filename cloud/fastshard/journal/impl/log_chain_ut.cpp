#include "log_chain.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

TLogRecordPtr MakeRecord(ui64 prevLsn, ui64 lsn)
{
    auto record = std::make_shared<TLogRecord>();
    record->PrevLsn = prevLsn;
    record->Lsn = lsn;
    return record;
}

// inserts a record and marks it as ready, the way a finished write lands in
// the chain
TLogRecordPtr InsertReady(TLogRecordChain& chain, ui64 prevLsn, ui64 lsn)
{
    auto record = MakeRecord(prevLsn, lsn);
    auto result = chain.Insert(record);
    UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetError().GetCode());
    UNIT_ASSERT_VALUES_EQUAL(record.get(), result.GetResult().get());
    UNIT_ASSERT(chain.MarkAsReady(prevLsn));
    return record;
}

// erases up to |lsn| the way cleanup does, once the watermark is known to
// sit inside the chained run
TVector<TLogRecordPtr> EraseUpTo(TLogRecordChain& chain, ui64 lsn)
{
    auto result = chain.EraseUpTo(lsn);
    UNIT_ASSERT_VALUES_EQUAL_C(
        S_OK,
        result.GetError().GetCode(),
        result.GetError().GetMessage());
    return result.ExtractResult();
}

TVector<ui64> GetLsns(const TVector<TLogRecordPtr>& records)
{
    TVector<ui64> lsns;
    for (const auto& record: records) {
        lsns.push_back(record->Lsn);
    }
    return lsns;
}

TVector<ui64> GetSortedLsns(const TVector<TLogRecordPtr>& records)
{
    auto lsns = GetLsns(records);
    Sort(lsns);
    return lsns;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLogRecordChainTest)
{
    Y_UNIT_TEST(ShouldRejectRecordWithPrevLsnNotBelowLsn)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            chain.Insert(MakeRecord(25, 20)).GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            chain.Insert(MakeRecord(20, 20)).GetError().GetCode());

        UNIT_ASSERT(!chain.GetOldest());
    }

    Y_UNIT_TEST(ShouldInsertChainedRecords)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 10, 20);
        InsertReady(chain, 20, 30);

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30}),
            GetLsns(chain.GetReadyRun(10, 10)));
    }

    Y_UNIT_TEST(ShouldAcceptRecordsInsertedOutOfOrder)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 20, 30);
        InsertReady(chain, 10, 20);

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30}),
            GetLsns(chain.GetReadyRun(10, 10)));
    }

    Y_UNIT_TEST(ShouldMarkRecordAsReadyByPrevLsn)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        // nothing to mark yet
        UNIT_ASSERT(!chain.MarkAsReady(10));

        chain.Insert(MakeRecord(10, 20));

        // a record that is not ready is held but invisible to the walk
        UNIT_ASSERT(!chain.GetNext(10));
        UNIT_ASSERT(!chain.GetOldest());

        UNIT_ASSERT(chain.MarkAsReady(10));
        UNIT_ASSERT_VALUES_EQUAL(20, chain.GetNext(10)->Lsn);
        UNIT_ASSERT_VALUES_EQUAL(20, chain.GetOldest()->Lsn);

        // marking twice is harmless
        UNIT_ASSERT(chain.MarkAsReady(10));
        UNIT_ASSERT_VALUES_EQUAL(20, chain.GetOldest()->Lsn);
    }

    Y_UNIT_TEST(ShouldExtendTheRunOnlyOverReadyRecords)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        chain.Insert(MakeRecord(10, 20));
        chain.Insert(MakeRecord(20, 30));

        // the second record is durable before the first one, so the run
        // cannot move past the first yet
        UNIT_ASSERT(chain.MarkAsReady(20));
        UNIT_ASSERT(!chain.GetOldest());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            chain.Insert(MakeRecord(15, 25)).GetError().GetCode());

        // marking the first one carries the run over both
        UNIT_ASSERT(chain.MarkAsReady(10));
        UNIT_ASSERT_VALUES_EQUAL(20, chain.GetOldest()->Lsn);
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            chain.Insert(MakeRecord(25, 35)).GetError().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30}),
            GetSortedLsns(EraseUpTo(chain, 30)));
    }

    Y_UNIT_TEST(ShouldReturnTheHeldRecordForAnExactDuplicate)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 10, 20);
        InsertReady(chain, 20, 30);

        // a duplicate is not an error: the chain hands back the record it
        // already holds, so the caller can wait on that one's promise
        auto first = chain.Insert(MakeRecord(10, 20));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, first.GetError().GetCode());
        UNIT_ASSERT(first.GetResult());
        UNIT_ASSERT_VALUES_EQUAL(20, first.GetResult()->Lsn);

        auto second = chain.Insert(MakeRecord(20, 30));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, second.GetError().GetCode());
        UNIT_ASSERT(second.GetResult());
        UNIT_ASSERT_VALUES_EQUAL(30, second.GetResult()->Lsn);
    }

    Y_UNIT_TEST(ShouldRejectConflictingRecordWithSameLsn)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 10, 20);

        // same lsn, different prev lsn
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            chain.Insert(MakeRecord(5, 20)).GetError().GetCode());

        // the original record is still the one held by the chain
        UNIT_ASSERT_VALUES_EQUAL(10, chain.GetOldest()->PrevLsn);
    }

    Y_UNIT_TEST(ShouldRejectForkFromTheSameLsn)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 10, 20);

        // two records cannot both continue from lsn 10
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            chain.Insert(MakeRecord(10, 25)).GetError().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20}),
            GetLsns(chain.GetReadyRun(10, 10)));
        UNIT_ASSERT_VALUES_EQUAL(20, chain.GetNext(10)->Lsn);
    }

    Y_UNIT_TEST(ShouldRejectRecordStartingInsideTheChainedRun)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 10, 20);
        InsertReady(chain, 20, 30);

        // every boundary of the run is held, so a record starting anywhere
        // else below its end starts inside another record
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            chain.Insert(MakeRecord(15, 25)).GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            chain.Insert(MakeRecord(25, 35)).GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            chain.Insert(MakeRecord(5, 15)).GetError().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30}),
            GetLsns(chain.GetReadyRun(10, 10)));
    }

    Y_UNIT_TEST(ShouldNotDetectOverlapPastAGap)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 10, 20);
        InsertReady(chain, 30, 40);

        // (35, 45] intersects (30, 40], but past the gap the chain only
        // looks up records by their boundaries, so this is the caller's
        // responsibility
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            chain.Insert(MakeRecord(35, 45)).GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldAcceptAbuttingRecords)
    {
        TLogRecordChain chain;

        InsertReady(chain, 10, 20);

        // (0, 10] abuts (10, 20] without intersecting it
        InsertReady(chain, 0, 10);

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{10, 20}),
            GetLsns(chain.GetReadyRun(0, 10)));
    }

    Y_UNIT_TEST(ShouldReturnNullptrFromGetOldestAndGetNextWhenEmpty)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        UNIT_ASSERT(!chain.GetOldest());
        UNIT_ASSERT(!chain.GetNext(0));
        UNIT_ASSERT(!chain.GetNext(100));
    }

    Y_UNIT_TEST(ShouldReturnTheLowestRecordAsOldest)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 20, 30);
        InsertReady(chain, 10, 20);

        UNIT_ASSERT_VALUES_EQUAL(20, chain.GetOldest()->Lsn);
    }

    Y_UNIT_TEST(ShouldRemoveRecordByPrevLsn)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        chain.Insert(MakeRecord(10, 20));
        chain.Insert(MakeRecord(20, 30));

        UNIT_ASSERT(chain.Remove(10));

        // removing an unknown prev lsn is reported, not ignored
        UNIT_ASSERT(!chain.Remove(10));
        UNIT_ASSERT(!chain.Remove(999));

        // the record after it is still held
        UNIT_ASSERT(chain.MarkAsReady(20));
        UNIT_ASSERT_VALUES_EQUAL(30, chain.GetNext(20)->Lsn);
        UNIT_ASSERT(!chain.MarkAsReady(10));

        // a ready record is part of the chain and cannot be taken out
        UNIT_ASSERT(!chain.Remove(20));
        UNIT_ASSERT_VALUES_EQUAL(30, chain.GetNext(20)->Lsn);
    }

    Y_UNIT_TEST(ShouldRestoreTheRunWhenARemovedRecordIsReinserted)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 10, 20);
        chain.Insert(MakeRecord(20, 30));   // its write is about to fail
        InsertReady(chain, 30, 40);

        // the failed write is taken out, the run still ends before it
        UNIT_ASSERT(chain.Remove(20));
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20}),
            GetLsns(chain.GetReadyRun(10, 10)));

        // the retry starts at the end of the run and rejoins the rest
        InsertReady(chain, 20, 30);
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30, 40}),
            GetLsns(chain.GetReadyRun(10, 10)));
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30, 40}),
            GetSortedLsns(EraseUpTo(chain, 40)));
    }

    Y_UNIT_TEST(ShouldExtendTheRunThroughWaitingRecords)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 30, 40);
        InsertReady(chain, 20, 30);

        // nothing chains from the watermark yet
        UNIT_ASSERT(!chain.GetOldest());

        // filling the gap joins everything behind it, so the run now covers
        // (10, 40] and guards against records starting inside it
        InsertReady(chain, 10, 20);
        UNIT_ASSERT_VALUES_EQUAL(20, chain.GetOldest()->Lsn);
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            chain.Insert(MakeRecord(35, 45)).GetError().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30, 40}),
            GetSortedLsns(EraseUpTo(chain, 40)));
    }

    Y_UNIT_TEST(ShouldGetRecordChainedFromLsn)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 10, 20);
        InsertReady(chain, 20, 30);

        // GetNext follows the chain: it returns the record whose prev lsn is
        // exactly the given lsn
        UNIT_ASSERT_VALUES_EQUAL(20, chain.GetNext(10)->Lsn);
        UNIT_ASSERT_VALUES_EQUAL(30, chain.GetNext(20)->Lsn);

        // nothing chains from an lsn that is not a record boundary
        UNIT_ASSERT(!chain.GetNext(0));
        UNIT_ASSERT(!chain.GetNext(15));
        UNIT_ASSERT(!chain.GetNext(25));

        // nothing chains from the last record
        UNIT_ASSERT(!chain.GetNext(30));
        UNIT_ASSERT(!chain.GetNext(100));
    }

    Y_UNIT_TEST(ShouldNotGetTheNextAcrossAGap)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 10, 20);
        InsertReady(chain, 30, 40);

        // (20, 30] is missing, so the chain stops at 20 even though a record
        // with a greater lsn exists
        UNIT_ASSERT(!chain.GetNext(20));
        UNIT_ASSERT_VALUES_EQUAL(40, chain.GetNext(30)->Lsn);
    }

    Y_UNIT_TEST(ShouldReturnAnEmptyRunWhenEmpty)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        UNIT_ASSERT(chain.GetReadyRun(0, 10).empty());
    }

    Y_UNIT_TEST(ShouldReturnTheReadyRunAfterLsn)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 10, 20);
        InsertReady(chain, 20, 30);
        InsertReady(chain, 30, 40);

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30, 40}),
            GetLsns(chain.GetReadyRun(10, 10)));

        // the tail continues from afterLsn, ascending
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{30, 40}),
            GetLsns(chain.GetReadyRun(20, 10)));
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{40}),
            GetLsns(chain.GetReadyRun(30, 10)));

        UNIT_ASSERT(chain.GetReadyRun(40, 10).empty());
        UNIT_ASSERT(chain.GetReadyRun(100, 10).empty());
    }

    Y_UNIT_TEST(ShouldLimitTheRunByMaxRecordCount)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 10, 20);
        InsertReady(chain, 20, 30);
        InsertReady(chain, 30, 40);

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20}),
            GetLsns(chain.GetReadyRun(10, 1)));
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30}),
            GetLsns(chain.GetReadyRun(10, 2)));
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{30}),
            GetLsns(chain.GetReadyRun(20, 1)));

        // asking for more than the chain holds is not an error
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30, 40}),
            GetLsns(chain.GetReadyRun(10, Max<ui64>())));
    }

    Y_UNIT_TEST(ShouldReturnAnEmptyRunWhenAfterLsnIsNotARecordBoundary)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 10, 20);
        InsertReady(chain, 20, 30);

        // the tail is anchored at afterLsn: nothing chains from 0, 15 or 25,
        // so no run can be returned without leaving a hole behind it
        UNIT_ASSERT(chain.GetReadyRun(0, 10).empty());
        UNIT_ASSERT(chain.GetReadyRun(15, 10).empty());
        UNIT_ASSERT(chain.GetReadyRun(25, 10).empty());
    }

    Y_UNIT_TEST(ShouldStopTheRunAtAGap)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 10, 20);
        InsertReady(chain, 30, 40);

        // (20, 30] is missing, so the tail ends at 20 rather than spanning it
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20}),
            GetLsns(chain.GetReadyRun(10, 10)));

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{40}),
            GetLsns(chain.GetReadyRun(30, 10)));
    }

    Y_UNIT_TEST(ShouldTreatZeroMaxRecordCountAsNoLimit)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 10, 20);
        InsertReady(chain, 20, 30);
        InsertReady(chain, 30, 40);

        // TReadJournalTailRequest documents 0 as "no limit"
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30, 40}),
            GetLsns(chain.GetReadyRun(10, 0)));

        UNIT_ASSERT(chain.GetReadyRun(40, 0).empty());
    }

    Y_UNIT_TEST(ShouldStopTheRunAtTheFirstRecordThatIsNotReady)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 10, 20);
        chain.Insert(MakeRecord(20, 30));   // still being written
        InsertReady(chain, 30, 40);

        // the tail stops at the record that is not ready instead of skipping
        // it and returning a hole
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20}),
            GetLsns(chain.GetReadyRun(10, 10)));

        // once it becomes durable the rest of the tail is visible
        UNIT_ASSERT(chain.MarkAsReady(20));
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30, 40}),
            GetLsns(chain.GetReadyRun(10, 10)));
    }

    Y_UNIT_TEST(ShouldReturnAnEmptyRunWhenTheFirstRecordIsNotReady)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        chain.Insert(MakeRecord(10, 20));
        InsertReady(chain, 20, 30);

        UNIT_ASSERT(chain.GetReadyRun(10, 10).empty());

        // a record that is not ready is invisible to GetOldest and GetNext as
        // well, but the ready ones behind it can still be reached
        UNIT_ASSERT(!chain.GetOldest());
        UNIT_ASSERT(!chain.GetNext(10));
        UNIT_ASSERT_VALUES_EQUAL(30, chain.GetNext(20)->Lsn);

        UNIT_ASSERT(chain.MarkAsReady(10));
        UNIT_ASSERT_VALUES_EQUAL(20, chain.GetOldest()->Lsn);
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30}),
            GetLsns(chain.GetReadyRun(10, 10)));
    }

    Y_UNIT_TEST(ShouldNotMarkARemovedRecordAsErased)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        chain.Insert(MakeRecord(10, 20));
        UNIT_ASSERT(chain.Remove(10));

        // Remove leaves LastErasedLsn alone, so the very same record is
        // accepted again rather than reported as already done
        auto record = MakeRecord(10, 20);
        auto result = chain.Insert(record);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(record.get(), result.GetResult().get());

        UNIT_ASSERT(chain.MarkAsReady(10));
        UNIT_ASSERT_VALUES_EQUAL(20, chain.GetOldest()->Lsn);
    }

    Y_UNIT_TEST(ShouldEraseRecordsUpToLsn)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 10, 20);
        InsertReady(chain, 20, 30);
        InsertReady(chain, 30, 40);

        // everything at or below lsn 30 is removed and handed back
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30}),
            GetSortedLsns(EraseUpTo(chain, 30)));
        UNIT_ASSERT_VALUES_EQUAL(40, chain.GetOldest()->Lsn);
    }

    Y_UNIT_TEST(ShouldEraseNothingBelowTheOldestLsn)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 10, 20);

        UNIT_ASSERT(EraseUpTo(chain, 19).empty());
        UNIT_ASSERT_VALUES_EQUAL(20, chain.GetOldest()->Lsn);

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20}),
            GetLsns(EraseUpTo(chain, 20)));
        UNIT_ASSERT(!chain.GetOldest());
    }

    Y_UNIT_TEST(ShouldDropReadyRecordsStrandedBelowTheWatermark)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        // both start inside (10, 20], which is not held yet, so the overlap
        // goes unnoticed
        auto ready = InsertReady(chain, 15, 25);
        chain.Insert(MakeRecord(18, 28));   // still being written

        InsertReady(chain, 10, 20);
        InsertReady(chain, 20, 30);

        // the head is erased, and so is the ready record that now starts
        // below the watermark: it can never join the chain
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 25}),
            GetSortedLsns(EraseUpTo(chain, 20)));

        // the chain does not touch promises, the caller fails them
        UNIT_ASSERT(!ready->Promise.Initialized());

        // the one that is not ready is left alone until its write finishes
        UNIT_ASSERT_VALUES_EQUAL(30, chain.GetOldest()->Lsn);
        UNIT_ASSERT(chain.MarkAsReady(18));
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{28}),
            GetLsns(EraseUpTo(chain, 20)));
        UNIT_ASSERT(!chain.MarkAsReady(18));
        UNIT_ASSERT_VALUES_EQUAL(30, chain.GetOldest()->Lsn);
    }

    Y_UNIT_TEST(ShouldRejectARecordAtOrBelowLastErasedLsn)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 10, 20);
        InsertReady(chain, 20, 30);
        InsertReady(chain, 30, 40);
        EraseUpTo(chain, 30);

        // an erased record is gone for good, the chain will not take it back
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            chain.Insert(MakeRecord(10, 20)).GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            chain.Insert(MakeRecord(20, 30)).GetError().GetCode());

        // nothing was inserted - the chain still starts where it did
        UNIT_ASSERT(!chain.MarkAsReady(10));
        UNIT_ASSERT_VALUES_EQUAL(40, chain.GetOldest()->Lsn);
    }

    Y_UNIT_TEST(ShouldTreatLsnsBelowInitLastErasedLsnAsErased)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(30);

        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            chain.Insert(MakeRecord(20, 30)).GetError().GetCode());
        UNIT_ASSERT(!chain.GetOldest());

        // the first record above the watermark is inserted as usual
        InsertReady(chain, 30, 40);
        UNIT_ASSERT_VALUES_EQUAL(40, chain.GetOldest()->Lsn);
    }

    Y_UNIT_TEST(ShouldRejectEraseUpToPastTheChainedRun)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 10, 20);
        chain.Insert(MakeRecord(20, 30));   // still being written
        InsertReady(chain, 30, 40);

        // the run ends at 20, so nothing past it can be erased yet and the
        // chain stays as it was
        auto result = chain.EraseUpTo(30);
        UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, result.GetError().GetCode());
        UNIT_ASSERT(result.GetResult().empty());
        UNIT_ASSERT_VALUES_EQUAL(20, chain.GetOldest()->Lsn);

        // once the gap is filled the same watermark goes through
        UNIT_ASSERT(chain.MarkAsReady(20));
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30}),
            GetSortedLsns(EraseUpTo(chain, 30)));
        UNIT_ASSERT_VALUES_EQUAL(40, chain.GetOldest()->Lsn);
    }

    Y_UNIT_TEST(ShouldNotRewindLastErasedLsnOnEraseUpTo)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(10);

        InsertReady(chain, 10, 20);
        InsertReady(chain, 20, 30);
        EraseUpTo(chain, 30);
        EraseUpTo(chain, 10);

        // the watermark stays at 30, so lsn 20 is still reported as erased
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            chain.Insert(MakeRecord(10, 20)).GetError().GetCode());
        UNIT_ASSERT(!chain.GetOldest());
    }
}

}   // namespace NCloud::NJournalled
