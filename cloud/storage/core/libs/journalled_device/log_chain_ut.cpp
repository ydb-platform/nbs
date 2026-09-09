#include "log_chain.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

namespace {

TLogRecordPtr MakeRecord(ui64 prevLsn, ui64 lsn, bool ready = true)
{
    auto record = std::make_shared<TLogRecord>();
    record->PrevLsn = prevLsn;
    record->Lsn = lsn;
    record->Ready = ready;
    return record;
}

TVector<ui64> GetLsns(const TVector<TLogRecordPtr>& records)
{
    TVector<ui64> lsns;
    for (const auto& record: records) {
        lsns.push_back(record->Lsn);
    }
    return lsns;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLogRecordChainTest)
{
    Y_UNIT_TEST(ShouldRejectRecordWithPrevLsnNotBelowLsn)
    {
        TLogRecordChain chain;

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

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            chain.Insert(MakeRecord(10, 20)).GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            chain.Insert(MakeRecord(20, 30)).GetError().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30}),
            GetLsns(chain.GetReadyRun(10, 10)));
    }

    Y_UNIT_TEST(ShouldAcceptRecordsInsertedOutOfOrder)
    {
        TLogRecordChain chain;

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            chain.Insert(MakeRecord(20, 30)).GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            chain.Insert(MakeRecord(10, 20)).GetError().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30}),
            GetLsns(chain.GetReadyRun(10, 10)));
    }

    Y_UNIT_TEST(ShouldReturnTheHeldRecordForAnExactDuplicate)
    {
        TLogRecordChain chain;

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            chain.Insert(MakeRecord(10, 20)).GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            chain.Insert(MakeRecord(20, 30)).GetError().GetCode());

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

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            chain.Insert(MakeRecord(10, 20)).GetError().GetCode());

        // same lsn, different prev lsn
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            chain.Insert(MakeRecord(5, 20)).GetError().GetCode());

        // the original record is still the one held by the chain
        UNIT_ASSERT_VALUES_EQUAL(10, chain.GetOldest()->PrevLsn);
    }

    Y_UNIT_TEST(ShouldRejectOverlapWithPreviousRecord)
    {
        TLogRecordChain chain;

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            chain.Insert(MakeRecord(10, 20)).GetError().GetCode());

        // (15, 25] intersects (10, 20]
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            chain.Insert(MakeRecord(15, 25)).GetError().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20}),
            GetLsns(chain.GetReadyRun(10, 10)));
    }

    Y_UNIT_TEST(ShouldRejectOverlapWithNextRecord)
    {
        TLogRecordChain chain;

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            chain.Insert(MakeRecord(10, 20)).GetError().GetCode());

        // (0, 15] intersects (10, 20] - the new record sorts before every
        // existing one, so only a check against the next record catches it
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            chain.Insert(MakeRecord(0, 15)).GetError().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20}),
            GetLsns(chain.GetReadyRun(10, 10)));
    }

    Y_UNIT_TEST(ShouldAcceptAbuttingRecords)
    {
        TLogRecordChain chain;

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            chain.Insert(MakeRecord(10, 20)).GetError().GetCode());

        // (0, 10] abuts (10, 20] without intersecting it
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            chain.Insert(MakeRecord(0, 10)).GetError().GetCode());

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{10, 20}),
            GetLsns(chain.GetReadyRun(0, 10)));
    }

    Y_UNIT_TEST(ShouldReturnNullptrFromGetOldestAndGetChainedNextWhenEmpty)
    {
        TLogRecordChain chain;

        UNIT_ASSERT(!chain.GetOldest());
        UNIT_ASSERT(!chain.GetChainedNext(0));
        UNIT_ASSERT(!chain.GetChainedNext(100));
    }

    Y_UNIT_TEST(ShouldReturnTheLowestRecordAsOldest)
    {
        TLogRecordChain chain;

        chain.Insert(MakeRecord(20, 30));
        chain.Insert(MakeRecord(10, 20));

        UNIT_ASSERT_VALUES_EQUAL(20, chain.GetOldest()->Lsn);
    }

    Y_UNIT_TEST(ShouldExtractRecordByLsn)
    {
        TLogRecordChain chain;

        chain.Insert(MakeRecord(10, 20));
        chain.Insert(MakeRecord(20, 30));

        // the removed record is handed back to the caller
        auto extracted = chain.Extract(20);
        UNIT_ASSERT(extracted);
        UNIT_ASSERT_VALUES_EQUAL(20, extracted->Lsn);
        UNIT_ASSERT_VALUES_EQUAL(30, chain.GetOldest()->Lsn);

        // removing an unknown lsn is a no-op
        UNIT_ASSERT(!chain.Extract(20));
        UNIT_ASSERT(!chain.Extract(999));
        UNIT_ASSERT_VALUES_EQUAL(30, chain.GetOldest()->Lsn);

        UNIT_ASSERT(chain.Extract(30));
        UNIT_ASSERT(!chain.GetOldest());
    }

    Y_UNIT_TEST(ShouldGetRecordChainedFromLsn)
    {
        TLogRecordChain chain;

        chain.Insert(MakeRecord(10, 20));
        chain.Insert(MakeRecord(20, 30));

        // GetNext follows the chain: it returns the record whose prev lsn is
        // exactly the given lsn
        UNIT_ASSERT_VALUES_EQUAL(20, chain.GetChainedNext(10)->Lsn);
        UNIT_ASSERT_VALUES_EQUAL(30, chain.GetChainedNext(20)->Lsn);

        // nothing chains from an lsn that is not a record boundary
        UNIT_ASSERT(!chain.GetChainedNext(0));
        UNIT_ASSERT(!chain.GetChainedNext(15));
        UNIT_ASSERT(!chain.GetChainedNext(25));

        // nothing chains from the last record
        UNIT_ASSERT(!chain.GetChainedNext(30));
        UNIT_ASSERT(!chain.GetChainedNext(100));
    }

    Y_UNIT_TEST(ShouldNotGetTheChainedNextAcrossAGap)
    {
        TLogRecordChain chain;

        chain.Insert(MakeRecord(10, 20));
        chain.Insert(MakeRecord(30, 40));

        // (20, 30] is missing, so the chain stops at 20 even though a record
        // with a greater lsn exists
        UNIT_ASSERT(!chain.GetChainedNext(20));
        UNIT_ASSERT_VALUES_EQUAL(40, chain.GetChainedNext(30)->Lsn);
    }

    Y_UNIT_TEST(ShouldReturnAnEmptyRunWhenEmpty)
    {
        TLogRecordChain chain;

        UNIT_ASSERT(chain.GetReadyRun(0, 10).empty());
    }

    Y_UNIT_TEST(ShouldReturnTheReadyRunAfterLsn)
    {
        TLogRecordChain chain;

        chain.Insert(MakeRecord(10, 20));
        chain.Insert(MakeRecord(20, 30));
        chain.Insert(MakeRecord(30, 40));

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

        chain.Insert(MakeRecord(10, 20));
        chain.Insert(MakeRecord(20, 30));
        chain.Insert(MakeRecord(30, 40));

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

        chain.Insert(MakeRecord(10, 20));
        chain.Insert(MakeRecord(20, 30));

        // the tail is anchored at afterLsn: nothing chains from 0, 15 or 25,
        // so no run can be returned without leaving a hole behind it
        UNIT_ASSERT(chain.GetReadyRun(0, 10).empty());
        UNIT_ASSERT(chain.GetReadyRun(15, 10).empty());
        UNIT_ASSERT(chain.GetReadyRun(25, 10).empty());
    }

    Y_UNIT_TEST(ShouldStopTheRunAtAGap)
    {
        TLogRecordChain chain;

        chain.Insert(MakeRecord(10, 20));
        chain.Insert(MakeRecord(30, 40));

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

        chain.Insert(MakeRecord(10, 20));
        chain.Insert(MakeRecord(20, 30));
        chain.Insert(MakeRecord(30, 40));

        // TReadJournalTailRequest documents 0 as "no limit"
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30, 40}),
            GetLsns(chain.GetReadyRun(10, 0)));

        UNIT_ASSERT(chain.GetReadyRun(40, 0).empty());
    }

    Y_UNIT_TEST(ShouldStopTheRunAtTheFirstDirtyRecord)
    {
        TLogRecordChain chain;

        chain.Insert(MakeRecord(10, 20));
        chain.Insert(MakeRecord(20, 30, false));   // dirty
        chain.Insert(MakeRecord(30, 40));

        // the tail stops at the dirty record instead of skipping it and
        // returning a hole
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20}),
            GetLsns(chain.GetReadyRun(10, 10)));

        // once it becomes durable the rest of the tail is visible
        chain.GetChainedNext(20)->Ready = true;
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30, 40}),
            GetLsns(chain.GetReadyRun(10, 10)));
    }

    Y_UNIT_TEST(ShouldReturnAnEmptyRunWhenTheFirstRecordIsDirty)
    {
        TLogRecordChain chain;

        chain.Insert(MakeRecord(10, 20, false));
        chain.Insert(MakeRecord(20, 30));

        UNIT_ASSERT(chain.GetReadyRun(10, 10).empty());

        // a dirty record is still reachable through GetOldest and
        // GetChainedNext - only the ready run filters on readiness
        UNIT_ASSERT_VALUES_EQUAL(20, chain.GetOldest()->Lsn);
        UNIT_ASSERT_VALUES_EQUAL(20, chain.GetChainedNext(10)->Lsn);
    }

    Y_UNIT_TEST(ShouldNotMarkAnExtractedRecordAsErased)
    {
        TLogRecordChain chain;

        chain.Insert(MakeRecord(10, 20));
        UNIT_ASSERT(chain.Extract(20));

        // Extract leaves LastErasedLsn alone, so the very same record is
        // accepted again rather than reported as already done
        auto record = MakeRecord(10, 20);
        auto result = chain.Insert(record);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(record.get(), result.GetResult().get());
        UNIT_ASSERT_VALUES_EQUAL(20, chain.GetOldest()->Lsn);
    }

    Y_UNIT_TEST(ShouldEraseRecordsUpToLsn)
    {
        TLogRecordChain chain;

        chain.Insert(MakeRecord(10, 20));
        chain.Insert(MakeRecord(20, 30));
        chain.Insert(MakeRecord(30, 40));

        // everything at or below lsn 30 is removed and handed back in order
        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20, 30}),
            GetLsns(chain.EraseUpTo(30)));
        UNIT_ASSERT_VALUES_EQUAL(40, chain.GetOldest()->Lsn);
    }

    Y_UNIT_TEST(ShouldEraseNothingBelowTheOldestLsn)
    {
        TLogRecordChain chain;

        chain.Insert(MakeRecord(10, 20));

        UNIT_ASSERT(chain.EraseUpTo(19).empty());
        UNIT_ASSERT_VALUES_EQUAL(20, chain.GetOldest()->Lsn);

        UNIT_ASSERT_VALUES_EQUAL(
            (TVector<ui64>{20}),
            GetLsns(chain.EraseUpTo(20)));
        UNIT_ASSERT(!chain.GetOldest());
    }

    Y_UNIT_TEST(ShouldReportAlreadyForARecordAtOrBelowLastErasedLsn)
    {
        TLogRecordChain chain;

        chain.Insert(MakeRecord(10, 20));
        chain.Insert(MakeRecord(20, 30));
        chain.Insert(MakeRecord(30, 40));
        chain.EraseUpTo(30);

        auto record = MakeRecord(10, 20);
        auto result = chain.Insert(record);

        // the caller gets a stub carrying the answer, not an error
        UNIT_ASSERT_VALUES_EQUAL(S_OK, result.GetError().GetCode());

        auto stub = result.GetResult();
        UNIT_ASSERT(stub);
        UNIT_ASSERT(stub.get() != record.get());
        UNIT_ASSERT_VALUES_EQUAL(20, stub->Lsn);
        UNIT_ASSERT_VALUES_EQUAL(10, stub->PrevLsn);
        UNIT_ASSERT(stub->Ready.load());

        auto future = stub->Promise.GetFuture();
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(S_ALREADY, future.GetValue().GetCode());

        // nothing was inserted - the chain still starts where it did
        UNIT_ASSERT_VALUES_EQUAL(40, chain.GetOldest()->Lsn);
    }

    Y_UNIT_TEST(ShouldTreatLsnsBelowInitLastErasedLsnAsErased)
    {
        TLogRecordChain chain;
        chain.InitLastErasedLsn(30);

        auto stub = chain.Insert(MakeRecord(20, 30)).GetResult();
        UNIT_ASSERT(stub);
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            stub->Promise.GetFuture().GetValue().GetCode());
        UNIT_ASSERT(!chain.GetOldest());

        // the first record above the watermark is inserted as usual
        auto record = MakeRecord(30, 40);
        UNIT_ASSERT_VALUES_EQUAL(
            record.get(),
            chain.Insert(record).GetResult().get());
        UNIT_ASSERT_VALUES_EQUAL(40, chain.GetOldest()->Lsn);
    }

    Y_UNIT_TEST(ShouldNotRewindLastErasedLsnOnEraseUpTo)
    {
        TLogRecordChain chain;

        chain.Insert(MakeRecord(10, 20));
        chain.Insert(MakeRecord(20, 30));
        chain.EraseUpTo(30);
        chain.EraseUpTo(10);

        // the watermark stays at 30, so lsn 20 is still reported as done
        auto stub = chain.Insert(MakeRecord(10, 20)).GetResult();
        UNIT_ASSERT(stub);
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            stub->Promise.GetFuture().GetValue().GetCode());
        UNIT_ASSERT(!chain.GetOldest());
    }
}

}   // namespace NCloud::NJournalled
