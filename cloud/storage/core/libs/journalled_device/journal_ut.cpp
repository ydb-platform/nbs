#include "journal.h"

#include "journal_store.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>

#include <latch>
#include <thread>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

// Forwards to a real store until the next call is armed to fail, so a single
// step of a write can be broken without disturbing the rest.
struct TFaultyPageStore final: public IPageStore
{
    IPageStorePtr Impl = CreateInMemoryPageStore();
    bool FailNextWrite = false;

    // lets a test park a write inside the store and release it later
    std::latch Entered{1};
    std::latch Release{1};
    bool BlockNextWrite = false;

    auto WritePageGroups(const NCloud::NProto::TWriteLogRecordRequest& request)
        -> TFutureResultOrError<TVector<TPageGroupRef>> override
    {
        if (BlockNextWrite) {
            BlockNextWrite = false;
            Entered.count_down();
            Release.wait();
        }

        if (FailNextWrite) {
            FailNextWrite = false;
            return NThreading::MakeFuture<TResultOrError<TVector<TPageGroupRef>>>(
                MakeError(E_IO, "page store is down"));
        }
        return Impl->WritePageGroups(request);
    }

    auto ReadPageGroups(const TVector<TPageGroupRef>& refs)
        -> TFutureResultOrError<TVector<TBuffer>> override
    {
        return Impl->ReadPageGroups(refs);
    }

    NCloud::NProto::TError Free(const TVector<TPageGroupRef>& refs) override
    {
        return Impl->Free(refs);
    }

    NCloud::NProto::TError MarkAsWritten(
        const TVector<TPageGroupRef>& refs) override
    {
        return Impl->MarkAsWritten(refs);
    }
};

////////////////////////////////////////////////////////////////////////////////

NCloud::NProto::TWriteLogRecordRequest MakeWrite(
    ui64 prevLsn,
    ui64 lsn,
    ui64 firstPageNo,
    const TVector<TString>& content)
{
    NCloud::NProto::TWriteLogRecordRequest request;
    request.SetPrevLogSequenceNumber(prevLsn);
    request.SetLogSequenceNumber(lsn);

    auto& group = *request.AddPageGroups();
    group.SetFirstPageNo(firstPageNo);
    for (const auto& page: content) {
        group.AddContent(page);
    }

    return request;
}

NCloud::NProto::TReadPagesRequest MakeRead(ui64 firstPageNo, ui64 pageCount)
{
    NCloud::NProto::TReadPagesRequest request;
    auto& ref = *request.AddPageGroupRefs();
    ref.SetFirstPageNo(firstPageNo);
    ref.SetPageCount(pageCount);
    return request;
}

TString Describe(const NCloud::NProto::TReadPagesResponse& response)
{
    TStringBuilder sb;
    for (const auto& group: response.GetPageGroups()) {
        sb << group.GetFirstPageNo() << ":";
        for (const auto& page: group.GetContent()) {
            sb << "[" << page << "]";
        }
        sb << " ";
    }
    return sb;
}

TString Lsns(const NCloud::NProto::TReadJournalTailResponse& response)
{
    TStringBuilder sb;
    for (const auto& record: response.GetRecords()) {
        if (sb) {
            sb << ",";
        }
        sb << record.GetLogSequenceNumber();
    }
    return sb;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TJournalTest)
{
    Y_UNIT_TEST(ShouldWriteAndReadBack)
    {
        TJournal journal(
            CreateInMemoryKeyBufferStore(),
            CreateInMemoryPageStore());

        auto write = journal.Write(MakeWrite(0, 10, 100, {"aaaa", "bbbb"}));
        UNIT_ASSERT(write.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, write.GetValue().GetCode());

        auto read = journal.Read(MakeRead(100, 2));
        UNIT_ASSERT_VALUES_EQUAL(
            "100:[aaaa][bbbb] ",
            Describe(read.GetValue()));
        UNIT_ASSERT_VALUES_EQUAL(
            10,
            read.GetValue().GetLastLogSequenceNumber());
    }

    Y_UNIT_TEST(ShouldServeTheNewestVersionOfAPage)
    {
        TJournal journal(
            CreateInMemoryKeyBufferStore(),
            CreateInMemoryPageStore());

        UNIT_ASSERT(journal.Write(
            MakeWrite(0, 10, 100, {"aaaa", "bbbb", "cccc"})).HasValue());
        UNIT_ASSERT(journal.Write(
            MakeWrite(10, 20, 101, {"XXXX"})).HasValue());

        auto read = journal.Read(MakeRead(100, 3));
        UNIT_ASSERT_VALUES_EQUAL(
            "100:[aaaa] 101:[XXXX] 102:[cccc] ",
            Describe(read.GetValue()));
    }

    Y_UNIT_TEST(ShouldFailARetryWhenTheOriginalWriteFails)
    {
        auto pageStore = std::make_shared<TFaultyPageStore>();
        TJournal journal(CreateInMemoryKeyBufferStore(), pageStore);

        // the first attempt is broken at the page store
        pageStore->FailNextWrite = true;
        auto first = journal.Write(MakeWrite(0, 10, 100, {"aaaa"}));
        UNIT_ASSERT(first.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(E_IO, first.GetValue().GetCode());

        // the record was rolled back, so a retry starts from scratch
        auto retry = journal.Write(MakeWrite(0, 10, 100, {"aaaa"}));
        UNIT_ASSERT(retry.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, retry.GetValue().GetCode());

        auto read = journal.Read(MakeRead(100, 1));
        UNIT_ASSERT_VALUES_EQUAL("100:[aaaa] ", Describe(read.GetValue()));
    }

    Y_UNIT_TEST(ShouldAnswerAConcurrentRetryWhenTheOriginalWriteFails)
    {
        auto pageStore = std::make_shared<TFaultyPageStore>();
        TJournal journal(CreateInMemoryKeyBufferStore(), pageStore);

        // park the first attempt inside the page store, then fail it
        pageStore->BlockNextWrite = true;
        pageStore->FailNextWrite = true;

        NCloud::NProto::TError firstError;
        std::thread writer(
            [&]
            {
                auto future = journal.Write(MakeWrite(0, 10, 100, {"aaaa"}));
                future.Wait();
                firstError = future.GetValue();
            });

        // the record is in the chain now, so a retry of the same lsn is
        // deduplicated onto the first attempt's promise
        pageStore->Entered.wait();
        auto retry = journal.Write(MakeWrite(0, 10, 100, {"aaaa"}));
        UNIT_ASSERT_C(!retry.HasValue(), "the retry waits on the original");

        pageStore->Release.count_down();
        writer.join();

        UNIT_ASSERT_VALUES_EQUAL(E_IO, firstError.GetCode());

        // the deduplicated caller has to be told the write failed, not left
        // waiting on a promise nobody will ever set
        UNIT_ASSERT_C(
            retry.Wait(TDuration::Seconds(5)),
            "the deduplicated retry must be answered");
        UNIT_ASSERT_VALUES_EQUAL(E_IO, retry.GetValue().GetCode());
    }

    Y_UNIT_TEST(ShouldNotLeavePendingWritesBehindAFailedRecord)
    {
        auto pageStore = std::make_shared<TFaultyPageStore>();
        TJournal journal(CreateInMemoryKeyBufferStore(), pageStore);

        // 20 is written first and cannot be indexed until 10 arrives
        auto second = journal.Write(MakeWrite(10, 20, 200, {"bbbb"}));
        UNIT_ASSERT_C(!second.HasValue(), "20 must wait for 10");

        // 10 then fails, so 20 keeps waiting rather than being answered wrongly
        pageStore->FailNextWrite = true;
        auto failed = journal.Write(MakeWrite(0, 10, 100, {"aaaa"}));
        UNIT_ASSERT(failed.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(E_IO, failed.GetValue().GetCode());
        UNIT_ASSERT_C(!second.HasValue(), "20 is still not indexable");

        // the retry of 10 links both records and answers both callers
        auto retried = journal.Write(MakeWrite(0, 10, 100, {"aaaa"}));
        UNIT_ASSERT(retried.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, retried.GetValue().GetCode());

        UNIT_ASSERT_C(second.HasValue(), "20 must be answered once 10 lands");
        UNIT_ASSERT_VALUES_EQUAL(S_OK, second.GetValue().GetCode());

        auto read = journal.Read(MakeRead(200, 1));
        UNIT_ASSERT_VALUES_EQUAL("200:[bbbb] ", Describe(read.GetValue()));
    }

    Y_UNIT_TEST(ShouldTailFromTheLastAckedLsn)
    {
        TJournal journal(
            CreateInMemoryKeyBufferStore(),
            CreateInMemoryPageStore());

        UNIT_ASSERT(journal.Write(MakeWrite(0, 10, 100, {"aaaa"})).HasValue());
        UNIT_ASSERT(journal.Write(MakeWrite(10, 20, 200, {"bbbb"})).HasValue());
        UNIT_ASSERT(journal.Write(MakeWrite(20, 30, 300, {"cccc"})).HasValue());

        UNIT_ASSERT_VALUES_EQUAL(
            "10,20,30",
            Lsns(journal.ReadTail(0, 0).GetValue()));

        auto advance = journal.AdvanceLastAckedLsn(20);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, advance.GetValue().GetCode());

        // the client has consumed through 20, so the tail starts after it and
        // says so in the response
        auto tail = journal.ReadTail(0, 0);
        UNIT_ASSERT_VALUES_EQUAL("30", Lsns(tail.GetValue()));
        UNIT_ASSERT_VALUES_EQUAL(
            20,
            tail.GetValue().GetLastAckedLogSequenceNumber());
    }

    Y_UNIT_TEST(ShouldFlushAndReclaimAckedRecords)
    {
        TJournal journal(
            CreateInMemoryKeyBufferStore(),
            CreateInMemoryPageStore());

        UNIT_ASSERT(journal.Write(MakeWrite(0, 10, 100, {"aaaa"})).HasValue());
        UNIT_ASSERT(journal.Write(MakeWrite(10, 20, 200, {"bbbb"})).HasValue());

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            journal.AdvanceLastAckedLsn(20).GetValue().GetCode());

        for (ui64 lsn: {10, 20}) {
            auto next = journal.GetFirstRecordToFlush();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, next.GetValue().GetError().GetCode());
            UNIT_ASSERT_VALUES_EQUAL(
                lsn,
                next.GetValue().GetResult().GetLogSequenceNumber());
            journal.MarkRecordAsFlushed(lsn);
        }

        // nothing acked is left to flush: an empty record, not an error
        auto empty = journal.GetFirstRecordToFlush().GetValue();
        UNIT_ASSERT_VALUES_EQUAL(S_OK, empty.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            empty.GetResult().GetLogSequenceNumber());
        UNIT_ASSERT_VALUES_EQUAL(0, empty.GetResult().GetPageGroups().size());

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            journal.CleanupFlushedRecords().GetValue().GetCode());

        // reclaimed pages are served from the device, not the journal
        UNIT_ASSERT_VALUES_EQUAL("", Describe(journal.Read(MakeRead(100, 1)).GetValue()));
    }

    Y_UNIT_TEST(ShouldRejectANonMonotonicAckedLsn)
    {
        TJournal journal(
            CreateInMemoryKeyBufferStore(),
            CreateInMemoryPageStore());

        UNIT_ASSERT(journal.Write(MakeWrite(0, 10, 100, {"aaaa"})).HasValue());
        UNIT_ASSERT(journal.Write(MakeWrite(10, 20, 200, {"bbbb"})).HasValue());

        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            journal.AdvanceLastAckedLsn(20).GetValue().GetCode());

        // a stale request must not reopen ground already acked
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            journal.AdvanceLastAckedLsn(10).GetValue().GetCode());

        // nor may it run past what has been indexed
        UNIT_ASSERT_VALUES_EQUAL(
            E_ARGUMENT,
            journal.AdvanceLastAckedLsn(30).GetValue().GetCode());
    }
}

////////////////////////////////////////////////////////////////////////////////

// CleanupFlushedRecords erases metadata before it frees pages. These two cases
// show why that order, and not the reverse, is the safe one: a crash in the
// middle must leave a journal that still starts.
Y_UNIT_TEST_SUITE(TJournalCleanupOrderTest)
{
    struct TFixture
    {
        IKeyBufferStorePtr MetaStore = CreateInMemoryKeyBufferStore();
        IPageStorePtr DataStore = CreateInMemoryPageStore();

        void FillAndFlush()
        {
            TJournal journal(MetaStore, DataStore);

            UNIT_ASSERT(journal.Write(
                MakeWrite(0, 10, 100, {"aaaa"})).HasValue());
            UNIT_ASSERT(journal.Write(
                MakeWrite(10, 20, 200, {"bbbb"})).HasValue());

            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                journal.AdvanceLastAckedLsn(20).GetValue().GetCode());

            for (ui64 lsn: {10, 20}) {
                UNIT_ASSERT_VALUES_EQUAL(
                    S_OK,
                    journal.GetFirstRecordToFlush()
                        .GetValue().GetError().GetCode());
                journal.MarkRecordAsFlushed(lsn);
            }
        }

        TVector<TPageGroupRef> RefsOf(ui64 lsn) const
        {
            auto buffer = MetaStore->Read(lsn).GetValue().GetResult();
            auto record = DeserializeRecord(buffer);
            UNIT_ASSERT(record);

            TVector<TPageGroupRef> refs;
            for (const auto& [pageNo, ref]: record->PageGroupIndex) {
                refs.push_back(ref);
            }
            return refs;
        }
    };

    Y_UNIT_TEST(ShouldStartAfterACrashBetweenMetaEraseAndPageFree)
    {
        TFixture f;
        f.FillAndFlush();

        // the metadata is gone but the pages were never freed
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            f.MetaStore->EraseTo(20).GetValue().GetCode());

        TJournal restarted(f.MetaStore, f.DataStore);
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            restarted.Restore().GetValue().GetCode());
    }

    Y_UNIT_TEST(ShouldFailToStartIfPagesWereFreedBeforeTheirMetadata)
    {
        TFixture f;
        f.FillAndFlush();

        // the reverse order: pages freed while the records still reference them
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            f.DataStore->Free(f.RefsOf(10)).GetCode());

        TJournal restarted(f.MetaStore, f.DataStore);
        UNIT_ASSERT_VALUES_EQUAL(
            E_NOT_FOUND,
            restarted.Restore().GetValue().GetCode());
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TJournalStressTest)
{
    Y_UNIT_TEST(ShouldAnswerEveryWriterWhileCleanupRuns)
    {
        // Write reads LastIndexedLsn outside the chain's lock, so concurrent
        // writers and cleanup can race it. Whatever the outcome of any single
        // write, no caller may be left holding a future that never completes.
        constexpr ui64 RecordCount = 400;

        TJournal journal(
            CreateInMemoryKeyBufferStore(),
            CreateInMemoryPageStore());

        std::atomic<bool> stop = false;
        std::thread cleaner(
            [&]
            {
                while (!stop.load(std::memory_order_relaxed)) {
                    auto acked = journal.AdvanceLastAckedLsn(
                        journal.ReadTail(0, 1).GetValue()
                            .GetLastAckedLogSequenceNumber() + 10);
                    Y_UNUSED(acked);

                    auto next = journal.GetFirstRecordToFlush();
                    if (!HasError(next.GetValue())) {
                        journal.MarkRecordAsFlushed(
                            next.GetValue().GetResult()
                                .GetLogSequenceNumber());
                    }

                    auto cleaned = journal.CleanupFlushedRecords();
                    Y_UNUSED(cleaned);
                }
            });

        TVector<NThreading::TFuture<NCloud::NProto::TError>> futures;
        futures.reserve(RecordCount);

        for (ui64 i = 1; i <= RecordCount; ++i) {
            futures.push_back(journal.Write(
                MakeWrite((i - 1) * 10, i * 10, i, {"aaaa"})));
        }

        stop.store(true, std::memory_order_relaxed);
        cleaner.join();

        // every write must have been answered one way or another
        ui64 unresolved = 0;
        for (auto& future: futures) {
            if (!future.Wait(TDuration::Seconds(10))) {
                ++unresolved;
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(0, unresolved);
    }
}

}   // namespace NCloud::NJournalled
