#include "log_index.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/map.h>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

namespace {

// TLogRecord holds an atomic, so it is neither copyable nor movable
TLogRecordPtr MakePageRecord(
    ui64 prevLsn,
    ui64 lsn,
    TVector<std::pair<ui64, TPageGroupRef>> pageGroupIndex)
{
    auto record = std::make_shared<TLogRecord>();
    record->PrevLsn = prevLsn;
    record->Lsn = lsn;
    record->PageGroupIndex = std::move(pageGroupIndex);
    return record;
}

// where a run of pages lives in the page store
TPageGroupRef Ref(ui64 firstPageNo, ui64 pageCount)
{
    return {.FirstPageNo = firstPageNo, .PageCount = pageCount};
}

// "<device page> -> <page store page> x<count>", in the order returned
TString Describe(const TVector<std::pair<ui64, TPageGroupRef>>& pages)
{
    TStringBuilder sb;
    for (const auto& [pageNo, ref]: pages) {
        if (sb) {
            sb << ", ";
        }
        sb << pageNo << "->" << ref.FirstPageNo << "x" << ref.PageCount;
    }
    return sb;
}

// everything the log holds, for asserting on the whole index at once
TString DescribeAll(const TLogPageMap& map)
{
    return Describe(map.GetIndex({Ref(0, 1000)}, 0).second);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLogPageMapTest)
{
    Y_UNIT_TEST(ShouldStartEmpty)
    {
        TLogPageMap map;

        UNIT_ASSERT_VALUES_EQUAL(0, map.GetLastIndexedLsn());
        UNIT_ASSERT(map.GetIndex({Ref(0, 4)}, 0).second.empty());
    }

    Y_UNIT_TEST(ShouldInitLastIndexedLsn)
    {
        TLogPageMap map;

        map.InitLastIndexedLsn(10);
        UNIT_ASSERT_VALUES_EQUAL(10, map.GetLastIndexedLsn());
    }

    Y_UNIT_TEST(ShouldRejectRecordThatDoesNotChainFromLastIndexedLsn)
    {
        TLogPageMap map;
        map.InitLastIndexedLsn(10);

        auto stale = MakePageRecord(0, 20, {{1, Ref(100, 1)}});
        auto ahead = MakePageRecord(15, 20, {{1, Ref(100, 1)}});

        UNIT_ASSERT(!map.AddNext(*stale));
        UNIT_ASSERT(!map.AddNext(*ahead));

        // rejected records leave the map untouched
        UNIT_ASSERT_VALUES_EQUAL(10, map.GetLastIndexedLsn());
        UNIT_ASSERT_VALUES_EQUAL("", DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldAdvanceLastIndexedLsnOnAddNext)
    {
        TLogPageMap map;
        map.InitLastIndexedLsn(10);

        auto first = MakePageRecord(10, 20, {{1, Ref(100, 1)}});
        auto second = MakePageRecord(20, 30, {{5, Ref(200, 1)}});
        auto stale = MakePageRecord(20, 40, {{9, Ref(300, 1)}});

        UNIT_ASSERT(map.AddNext(*first));
        UNIT_ASSERT_VALUES_EQUAL(20, map.GetLastIndexedLsn());

        UNIT_ASSERT(map.AddNext(*second));
        UNIT_ASSERT_VALUES_EQUAL(30, map.GetLastIndexedLsn());

        // the chain continues from the lsn just applied
        UNIT_ASSERT(!map.AddNext(*stale));
        UNIT_ASSERT_VALUES_EQUAL(30, map.GetLastIndexedLsn());
    }

    Y_UNIT_TEST(ShouldKeepDisjointRanges)
    {
        TLogPageMap map;
        map.InitLastIndexedLsn(10);

        auto record = MakePageRecord(
            10,
            20,
            {{1, Ref(100, 2)}, {5, Ref(200, 3)}});
        UNIT_ASSERT(map.AddNext(*record));

        UNIT_ASSERT_VALUES_EQUAL("1->100x2, 5->200x3", DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldSkipPagesThatAreNotInTheLog)
    {
        TLogPageMap map;
        map.InitLastIndexedLsn(10);

        auto record = MakePageRecord(10, 20, {{2, Ref(100, 2)}});
        UNIT_ASSERT(map.AddNext(*record));

        // pages 0, 1, 4 and 5 live only on the device, so they are left out
        UNIT_ASSERT_VALUES_EQUAL("2->100x2", Describe(map.GetIndex({Ref(0, 6)}, 0).second));
    }

    Y_UNIT_TEST(ShouldOverrideAnOlderRangeEntirely)
    {
        TLogPageMap map;
        map.InitLastIndexedLsn(10);

        auto older = MakePageRecord(10, 20, {{1, Ref(100, 4)}});
        auto newer = MakePageRecord(20, 30, {{1, Ref(200, 4)}});

        UNIT_ASSERT(map.AddNext(*older));
        UNIT_ASSERT(map.AddNext(*newer));

        UNIT_ASSERT_VALUES_EQUAL("1->200x4", DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldOverrideAnOlderRangeThatItContains)
    {
        TLogPageMap map;
        map.InitLastIndexedLsn(10);

        auto older = MakePageRecord(10, 20, {{3, Ref(100, 2)}});
        auto newer = MakePageRecord(20, 30, {{1, Ref(200, 8)}});

        UNIT_ASSERT(map.AddNext(*older));
        UNIT_ASSERT(map.AddNext(*newer));

        UNIT_ASSERT_VALUES_EQUAL("1->200x8", DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldTrimTheTailOfAnOlderRange)
    {
        TLogPageMap map;
        map.InitLastIndexedLsn(10);

        // device [1, 5) -> store [100, 104)
        auto older = MakePageRecord(10, 20, {{1, Ref(100, 4)}});
        // device [3, 7) -> store [200, 204)
        auto newer = MakePageRecord(20, 30, {{3, Ref(200, 4)}});

        UNIT_ASSERT(map.AddNext(*older));
        UNIT_ASSERT(map.AddNext(*newer));

        // the old range keeps only [1, 3), still at store 100
        UNIT_ASSERT_VALUES_EQUAL("1->100x2, 3->200x4", DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldTrimTheHeadOfAnOlderRangeAndMoveItsOffset)
    {
        TLogPageMap map;
        map.InitLastIndexedLsn(10);

        // device [3, 7) -> store [100, 104)
        auto older = MakePageRecord(10, 20, {{3, Ref(100, 4)}});
        // device [1, 5) -> store [200, 204)
        auto newer = MakePageRecord(20, 30, {{1, Ref(200, 4)}});

        UNIT_ASSERT(map.AddNext(*older));
        UNIT_ASSERT(map.AddNext(*newer));

        // the old range keeps [5, 7); device page 5 was the third page of the
        // old group, so it now points at store page 102
        UNIT_ASSERT_VALUES_EQUAL("1->200x4, 5->102x2", DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldSplitAnOlderRangeInTwo)
    {
        TLogPageMap map;
        map.InitLastIndexedLsn(10);

        // device [1, 9) -> store [100, 108)
        auto older = MakePageRecord(10, 20, {{1, Ref(100, 8)}});
        // device [4, 6) -> store [200, 202)
        auto newer = MakePageRecord(20, 30, {{4, Ref(200, 2)}});

        UNIT_ASSERT(map.AddNext(*older));
        UNIT_ASSERT(map.AddNext(*newer));

        // [1, 4) keeps store 100, [6, 9) picks up store 105
        UNIT_ASSERT_VALUES_EQUAL(
            "1->100x3, 4->200x2, 6->105x3",
            DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldLeaveAbuttingRangesAlone)
    {
        TLogPageMap map;
        map.InitLastIndexedLsn(10);

        auto older = MakePageRecord(10, 20, {{1, Ref(100, 4)}});
        auto newer = MakePageRecord(20, 30, {{5, Ref(200, 4)}});

        UNIT_ASSERT(map.AddNext(*older));
        UNIT_ASSERT(map.AddNext(*newer));

        // [1, 5) and [5, 9) touch but do not intersect
        UNIT_ASSERT_VALUES_EQUAL("1->100x4, 5->200x4", DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldTrimSeveralOlderRangesAtOnce)
    {
        TLogPageMap map;
        map.InitLastIndexedLsn(10);

        auto first = MakePageRecord(
            10,
            20,
            {{1, Ref(100, 3)}, {6, Ref(200, 2)}, {10, Ref(300, 3)}});
        // device [2, 11) swallows the middle range and clips the outer two
        auto second = MakePageRecord(20, 30, {{2, Ref(400, 9)}});

        UNIT_ASSERT(map.AddNext(*first));
        UNIT_ASSERT(map.AddNext(*second));

        UNIT_ASSERT_VALUES_EQUAL(
            "1->100x1, 2->400x9, 11->301x2",
            DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldClipTheResultToTheRequestedRange)
    {
        TLogPageMap map;
        map.InitLastIndexedLsn(10);

        // device [1, 9) -> store [100, 108)
        auto record = MakePageRecord(10, 20, {{1, Ref(100, 8)}});
        UNIT_ASSERT(map.AddNext(*record));

        // asking for [3, 5) yields only that slice of the stored group
        UNIT_ASSERT_VALUES_EQUAL("3->102x2", Describe(map.GetIndex({Ref(3, 2)}, 0).second));

        // a request reaching past both ends is clipped to what the log holds
        UNIT_ASSERT_VALUES_EQUAL("1->100x8", Describe(map.GetIndex({Ref(0, 20)}, 0).second));

        // several requested groups are served in request order
        UNIT_ASSERT_VALUES_EQUAL(
            "6->105x1, 2->101x1",
            Describe(map.GetIndex({Ref(6, 1), Ref(2, 1)}, 0).second));
    }

    Y_UNIT_TEST(ShouldIgnoreRangesAtOrBelowAfterLsn)
    {
        TLogPageMap map;
        map.InitLastIndexedLsn(10);

        auto older = MakePageRecord(10, 20, {{1, Ref(100, 2)}});
        auto newer = MakePageRecord(20, 30, {{5, Ref(200, 2)}});

        UNIT_ASSERT(map.AddNext(*older));
        UNIT_ASSERT(map.AddNext(*newer));

        UNIT_ASSERT_VALUES_EQUAL(
            "1->100x2, 5->200x2",
            Describe(map.GetIndex({Ref(0, 10)}, 0).second));

        // the older range may already have been reclaimed
        UNIT_ASSERT_VALUES_EQUAL(
            "5->200x2",
            Describe(map.GetIndex({Ref(0, 10)}, 20).second));
    }

    Y_UNIT_TEST(ShouldReturnNothingWhenAfterLsnIsAtOrBeyondLastIndexedLsn)
    {
        TLogPageMap map;
        map.InitLastIndexedLsn(10);

        auto record = MakePageRecord(10, 20, {{1, Ref(100, 1)}});
        UNIT_ASSERT(map.AddNext(*record));

        UNIT_ASSERT(map.GetIndex({Ref(1, 1)}, 20).second.empty());
        UNIT_ASSERT(map.GetIndex({Ref(1, 1)}, 100).second.empty());
    }

    Y_UNIT_TEST(ShouldNotLetADegenerateEntryBlockALaterMapping)
    {
        TLogPageMap map;
        map.InitLastIndexedLsn(10);

        // a group covering no pages at all
        auto empty = MakePageRecord(10, 20, {{5, Ref(100, 0)}});
        UNIT_ASSERT(map.AddNext(*empty));

        // a later record maps that same first page for real
        auto real = MakePageRecord(20, 30, {{5, Ref(200, 3)}});
        UNIT_ASSERT(map.AddNext(*real));

        // the real mapping must win rather than be dropped on a taken key
        UNIT_ASSERT_VALUES_EQUAL("5->200x3", DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldIgnoreADegenerateEntryWhenReading)
    {
        TLogPageMap map;
        map.InitLastIndexedLsn(10);

        auto record = MakePageRecord(
            10,
            20,
            {{1, Ref(100, 2)}, {7, Ref(200, 0)}});
        UNIT_ASSERT(map.AddNext(*record));

        // the zero length group contributes nothing to what can be served
        UNIT_ASSERT_VALUES_EQUAL("1->100x2", DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldRemoveEverythingUpToTheGivenLsn)
    {
        TLogPageMap map;
        map.InitLastIndexedLsn(10);

        auto first = MakePageRecord(10, 20, {{1, Ref(100, 2)}});
        auto second = MakePageRecord(20, 30, {{5, Ref(200, 2)}});
        auto third = MakePageRecord(30, 40, {{9, Ref(300, 2)}});

        UNIT_ASSERT(map.AddNext(*first));
        UNIT_ASSERT(map.AddNext(*second));
        UNIT_ASSERT(map.AddNext(*third));

        map.EraseTo(30);

        // everything at or below lsn 30 is gone, lsn 40 stays
        UNIT_ASSERT_VALUES_EQUAL("9->300x2", DescribeAll(map));

        // removing does not rewind the applied position
        UNIT_ASSERT_VALUES_EQUAL(40, map.GetLastIndexedLsn());
    }

    Y_UNIT_TEST(ShouldRemoveEverythingWhenTrimmingPastLastIndexedLsn)
    {
        TLogPageMap map;
        map.InitLastIndexedLsn(10);

        auto record = MakePageRecord(10, 20, {{1, Ref(100, 2)}});
        UNIT_ASSERT(map.AddNext(*record));

        // trimming beyond the applied position drops everything applied so far
        map.EraseTo(30);
        UNIT_ASSERT_VALUES_EQUAL("", DescribeAll(map));

        // and it does not rewind the applied position
        UNIT_ASSERT_VALUES_EQUAL(20, map.GetLastIndexedLsn());
    }

    Y_UNIT_TEST(ShouldRemoveNothingBelowTheOldestLsn)
    {
        TLogPageMap map;
        map.InitLastIndexedLsn(10);

        auto record = MakePageRecord(10, 20, {{1, Ref(100, 2)}});
        UNIT_ASSERT(map.AddNext(*record));

        map.EraseTo(19);
        UNIT_ASSERT_VALUES_EQUAL("1->100x2", DescribeAll(map));

        map.EraseTo(20);
        UNIT_ASSERT_VALUES_EQUAL("", DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldBeReadableThroughAConstReference)
    {
        TLogPageMap map;
        map.InitLastIndexedLsn(10);

        auto record = MakePageRecord(10, 20, {{1, Ref(100, 2)}});
        UNIT_ASSERT(map.AddNext(*record));

        const auto& constMap = map;
        UNIT_ASSERT_VALUES_EQUAL(20, constMap.GetLastIndexedLsn());
        UNIT_ASSERT_VALUES_EQUAL(
            "1->100x2",
            Describe(constMap.GetIndex({Ref(1, 2)}, 0).second));
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

// brute-force model: one entry per device page, no ranges at all
using TModel = TMap<ui64, std::pair<ui64 /*lsn*/, ui64 /*storePage*/>>;

void ModelApply(TModel& model, ui64 lsn, const TVector<std::pair<ui64, TPageGroupRef>>& groups)
{
    for (const auto& [pageNo, ref]: groups) {
        for (ui64 i = 0; i < ref.PageCount; ++i) {
            model[pageNo + i] = {lsn, ref.FirstPageNo + i};
        }
    }
}

void ModelEraseTo(TModel& model, ui64 lsn)
{
    for (auto it = model.begin(); it != model.end();) {
        it = it->second.first <= lsn ? model.erase(it) : std::next(it);
    }
}

TVector<std::pair<ui64, ui64>> ModelGet(const TModel& model, ui64 from, ui64 to, ui64 afterLsn)
{
    TVector<std::pair<ui64, ui64>> out;
    for (ui64 p = from; p < to; ++p) {
        auto it = model.find(p);
        if (it != model.end() && it->second.first > afterLsn) {
            out.emplace_back(p, it->second.second);
        }
    }
    return out;
}

// expand the range-based answer back to one entry per page
TVector<std::pair<ui64, ui64>> Expand(const TVector<std::pair<ui64, TPageGroupRef>>& pages)
{
    TVector<std::pair<ui64, ui64>> out;
    for (const auto& [pageNo, ref]: pages) {
        for (ui64 i = 0; i < ref.PageCount; ++i) {
            out.emplace_back(pageNo + i, ref.FirstPageNo + i);
        }
    }
    return out;
}

TString Show(const TVector<std::pair<ui64, ui64>>& v)
{
    TStringBuilder sb;
    for (const auto& [a, b]: v) {
        if (sb) { sb << " "; }
        sb << a << "->" << b;
    }
    return sb;
}

struct TRng
{
    ui64 S = 88172645463325252ull;
    ui64 Next(ui64 n) { S ^= S << 13; S ^= S >> 7; S ^= S << 17; return S % n; }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLogPageMapModelTest)
{
    Y_UNIT_TEST(DifferentialAgainstPerPageModel)
    {
        constexpr ui64 PageSpace = 48;
        ui64 mismatches = 0;

        for (ui64 seed = 1; seed <= 500 && mismatches < 5; ++seed) {
            TRng rng{seed * 6364136223846793005ull + 1442695040888963407ull};

            TLogPageMap map;
            TModel model;
            ui64 lsn = 0;
            ui64 storeNext = 1000;

            for (ui64 step = 0; step < 40 && mismatches < 5; ++step) {
                // apply one record made of 1..3 page groups
                TVector<std::pair<ui64, TPageGroupRef>> groups;
                const ui64 groupCnt = 1 + rng.Next(3);
                for (ui64 g = 0; g < groupCnt; ++g) {
                    const ui64 len = 1 + rng.Next(8);
                    const ui64 start = rng.Next(PageSpace - len);
                    groups.push_back(
                        {start, TPageGroupRef{.FirstPageNo = storeNext, .PageCount = len}});
                    storeNext += len;
                }

                auto record = std::make_shared<TLogRecord>();
                record->PrevLsn = lsn;
                record->Lsn = lsn + 10;
                record->PageGroupIndex = groups;
                UNIT_ASSERT(map.AddNext(*record));
                lsn += 10;
                ModelApply(model, lsn, groups);

                if (rng.Next(6) == 0) {
                    const ui64 upTo = rng.Next(lsn + 20);
                    map.EraseTo(upTo);
                    ModelEraseTo(model, upTo);
                }

                // query random windows with random afterLsn
                for (ui64 q = 0; q < 6; ++q) {
                    const ui64 len = 1 + rng.Next(PageSpace);
                    const ui64 from = rng.Next(PageSpace + 8);
                    const ui64 afterLsn = rng.Next(lsn + 20);

                    const auto got = Expand(
                        map.GetIndex(
                            {TPageGroupRef{
                                .FirstPageNo = from,
                                .PageCount = len}},
                            afterLsn).second);
                    const auto want = ModelGet(model, from, from + len, afterLsn);

                    if (Show(got) != Show(want)) {
                        ++mismatches;
                        Cerr << "\nMISMATCH seed=" << seed << " step=" << step
                            << " query=[" << from << "," << (from + len)
                            << ") afterLsn=" << afterLsn
                            << " lastIndexedLsn=" << map.GetLastIndexedLsn() << "\n"
                            << "  got : " << Show(got) << "\n"
                            << "  want: " << Show(want) << "\n";
                    }
                }
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(0, mismatches);
    }
}

}   // namespace NCloud::NJournalled
