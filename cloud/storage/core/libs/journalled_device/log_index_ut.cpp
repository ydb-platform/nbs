#include "log_index.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/map.h>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

TLogRecordPtr
MakePageRecord(ui64 prevLsn, ui64 lsn, TVector<TPageMapping> pageMappings)
{
    auto record = std::make_shared<TLogRecord>();
    record->PrevLsn = prevLsn;
    record->Lsn = lsn;
    record->PageMappings = std::move(pageMappings);
    return record;
}

// a run of pages - in the page store when it is the location a mapping points
// at, in the device address space when it is what a reader asks for
TPageRange Range(ui64 firstPageNo, ui64 pageCount)
{
    return {.FirstPageNo = firstPageNo, .PageCount = pageCount};
}

// "<device page> -> <page store page> x<count>", in the order returned
TString Describe(const TVector<TPageMapping>& mappings)
{
    TStringBuilder sb;
    for (const auto& [pageNo, location]: mappings) {
        if (sb) {
            sb << ", ";
        }
        sb << pageNo << "->" << location.FirstPageNo << "x"
           << location.PageCount;
    }
    return sb;
}

// everything the log holds, for asserting on the whole index at once
TString DescribeAll(const TLogPageIndex& map)
{
    return Describe(map.Lookup({Range(0, 1000)}, 0).Mappings);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLogPageIndexTest)
{
    Y_UNIT_TEST(ShouldStartEmpty)
    {
        TLogPageIndex map;

        UNIT_ASSERT_VALUES_EQUAL(0, map.GetLastIndexedLsn());
        UNIT_ASSERT(map.Lookup({Range(0, 4)}, 0).Mappings.empty());
    }

    Y_UNIT_TEST(ShouldInitLastIndexedLsn)
    {
        TLogPageIndex map;

        map.InitLastIndexedLsn(10);
        UNIT_ASSERT_VALUES_EQUAL(10, map.GetLastIndexedLsn());
    }

    Y_UNIT_TEST(ShouldRejectRecordThatDoesNotChainFromLastIndexedLsn)
    {
        TLogPageIndex map;
        map.InitLastIndexedLsn(10);

        auto stale = MakePageRecord(0, 20, {{1, Range(100, 1)}});
        auto ahead = MakePageRecord(15, 20, {{1, Range(100, 1)}});

        UNIT_ASSERT(!map.TryApplyNext(*stale));
        UNIT_ASSERT(!map.TryApplyNext(*ahead));

        // rejected records leave the map untouched
        UNIT_ASSERT_VALUES_EQUAL(10, map.GetLastIndexedLsn());
        UNIT_ASSERT_VALUES_EQUAL("", DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldAdvanceLastIndexedLsnOnTryApplyNext)
    {
        TLogPageIndex map;
        map.InitLastIndexedLsn(10);

        auto first = MakePageRecord(10, 20, {{1, Range(100, 1)}});
        auto second = MakePageRecord(20, 30, {{5, Range(200, 1)}});
        auto stale = MakePageRecord(20, 40, {{9, Range(300, 1)}});

        UNIT_ASSERT(map.TryApplyNext(*first));
        UNIT_ASSERT_VALUES_EQUAL(20, map.GetLastIndexedLsn());

        UNIT_ASSERT(map.TryApplyNext(*second));
        UNIT_ASSERT_VALUES_EQUAL(30, map.GetLastIndexedLsn());

        // the chain continues from the lsn just applied
        UNIT_ASSERT(!map.TryApplyNext(*stale));
        UNIT_ASSERT_VALUES_EQUAL(30, map.GetLastIndexedLsn());
    }

    Y_UNIT_TEST(ShouldKeepDisjointRanges)
    {
        TLogPageIndex map;
        map.InitLastIndexedLsn(10);

        auto record =
            MakePageRecord(10, 20, {{1, Range(100, 2)}, {5, Range(200, 3)}});
        UNIT_ASSERT(map.TryApplyNext(*record));

        UNIT_ASSERT_VALUES_EQUAL("1->100x2, 5->200x3", DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldSkipPagesThatAreNotInTheLog)
    {
        TLogPageIndex map;
        map.InitLastIndexedLsn(10);

        auto record = MakePageRecord(10, 20, {{2, Range(100, 2)}});
        UNIT_ASSERT(map.TryApplyNext(*record));

        // pages 0, 1, 4 and 5 live only on the device, so they are left out
        UNIT_ASSERT_VALUES_EQUAL(
            "2->100x2",
            Describe(map.Lookup({Range(0, 6)}, 0).Mappings));
    }

    Y_UNIT_TEST(ShouldOverrideAnOlderRangeEntirely)
    {
        TLogPageIndex map;
        map.InitLastIndexedLsn(10);

        auto older = MakePageRecord(10, 20, {{1, Range(100, 4)}});
        auto newer = MakePageRecord(20, 30, {{1, Range(200, 4)}});

        UNIT_ASSERT(map.TryApplyNext(*older));
        UNIT_ASSERT(map.TryApplyNext(*newer));

        UNIT_ASSERT_VALUES_EQUAL("1->200x4", DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldOverrideAnOlderRangeThatItContains)
    {
        TLogPageIndex map;
        map.InitLastIndexedLsn(10);

        auto older = MakePageRecord(10, 20, {{3, Range(100, 2)}});
        auto newer = MakePageRecord(20, 30, {{1, Range(200, 8)}});

        UNIT_ASSERT(map.TryApplyNext(*older));
        UNIT_ASSERT(map.TryApplyNext(*newer));

        UNIT_ASSERT_VALUES_EQUAL("1->200x8", DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldTrimTheTailOfAnOlderRange)
    {
        TLogPageIndex map;
        map.InitLastIndexedLsn(10);

        // device [1, 5) -> store [100, 104)
        auto older = MakePageRecord(10, 20, {{1, Range(100, 4)}});
        // device [3, 7) -> store [200, 204)
        auto newer = MakePageRecord(20, 30, {{3, Range(200, 4)}});

        UNIT_ASSERT(map.TryApplyNext(*older));
        UNIT_ASSERT(map.TryApplyNext(*newer));

        // the old range keeps only [1, 3), still at store 100
        UNIT_ASSERT_VALUES_EQUAL("1->100x2, 3->200x4", DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldTrimTheHeadOfAnOlderRangeAndMoveItsOffset)
    {
        TLogPageIndex map;
        map.InitLastIndexedLsn(10);

        // device [3, 7) -> store [100, 104)
        auto older = MakePageRecord(10, 20, {{3, Range(100, 4)}});
        // device [1, 5) -> store [200, 204)
        auto newer = MakePageRecord(20, 30, {{1, Range(200, 4)}});

        UNIT_ASSERT(map.TryApplyNext(*older));
        UNIT_ASSERT(map.TryApplyNext(*newer));

        // the old range keeps [5, 7); device page 5 was the third page of the
        // old group, so it now points at store page 102
        UNIT_ASSERT_VALUES_EQUAL("1->200x4, 5->102x2", DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldSplitAnOlderRangeInTwo)
    {
        TLogPageIndex map;
        map.InitLastIndexedLsn(10);

        // device [1, 9) -> store [100, 108)
        auto older = MakePageRecord(10, 20, {{1, Range(100, 8)}});
        // device [4, 6) -> store [200, 202)
        auto newer = MakePageRecord(20, 30, {{4, Range(200, 2)}});

        UNIT_ASSERT(map.TryApplyNext(*older));
        UNIT_ASSERT(map.TryApplyNext(*newer));

        // [1, 4) keeps store 100, [6, 9) picks up store 105
        UNIT_ASSERT_VALUES_EQUAL(
            "1->100x3, 4->200x2, 6->105x3",
            DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldLeaveAbuttingRangesAlone)
    {
        TLogPageIndex map;
        map.InitLastIndexedLsn(10);

        auto older = MakePageRecord(10, 20, {{1, Range(100, 4)}});
        auto newer = MakePageRecord(20, 30, {{5, Range(200, 4)}});

        UNIT_ASSERT(map.TryApplyNext(*older));
        UNIT_ASSERT(map.TryApplyNext(*newer));

        // [1, 5) and [5, 9) touch but do not intersect
        UNIT_ASSERT_VALUES_EQUAL("1->100x4, 5->200x4", DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldTrimSeveralOlderRangesAtOnce)
    {
        TLogPageIndex map;
        map.InitLastIndexedLsn(10);

        auto first = MakePageRecord(
            10,
            20,
            {{1, Range(100, 3)}, {6, Range(200, 2)}, {10, Range(300, 3)}});
        // device [2, 11) swallows the middle range and clips the outer two
        auto second = MakePageRecord(20, 30, {{2, Range(400, 9)}});

        UNIT_ASSERT(map.TryApplyNext(*first));
        UNIT_ASSERT(map.TryApplyNext(*second));

        UNIT_ASSERT_VALUES_EQUAL(
            "1->100x1, 2->400x9, 11->301x2",
            DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldClipTheResultToTheRequestedRange)
    {
        TLogPageIndex map;
        map.InitLastIndexedLsn(10);

        // device [1, 9) -> store [100, 108)
        auto record = MakePageRecord(10, 20, {{1, Range(100, 8)}});
        UNIT_ASSERT(map.TryApplyNext(*record));

        // asking for [3, 5) yields only that slice of the stored group
        UNIT_ASSERT_VALUES_EQUAL(
            "3->102x2",
            Describe(map.Lookup({Range(3, 2)}, 0).Mappings));

        // a request reaching past both ends is clipped to what the log holds
        UNIT_ASSERT_VALUES_EQUAL(
            "1->100x8",
            Describe(map.Lookup({Range(0, 20)}, 0).Mappings));

        // several requested groups are served in request order
        UNIT_ASSERT_VALUES_EQUAL(
            "6->105x1, 2->101x1",
            Describe(map.Lookup({Range(6, 1), Range(2, 1)}, 0).Mappings));
    }

    Y_UNIT_TEST(ShouldIgnoreRangesAtOrBelowAfterLsn)
    {
        TLogPageIndex map;
        map.InitLastIndexedLsn(10);

        auto older = MakePageRecord(10, 20, {{1, Range(100, 2)}});
        auto newer = MakePageRecord(20, 30, {{5, Range(200, 2)}});

        UNIT_ASSERT(map.TryApplyNext(*older));
        UNIT_ASSERT(map.TryApplyNext(*newer));

        UNIT_ASSERT_VALUES_EQUAL(
            "1->100x2, 5->200x2",
            Describe(map.Lookup({Range(0, 10)}, 0).Mappings));

        // the older range may already have been reclaimed
        UNIT_ASSERT_VALUES_EQUAL(
            "5->200x2",
            Describe(map.Lookup({Range(0, 10)}, 20).Mappings));
    }

    Y_UNIT_TEST(ShouldReturnNothingWhenAfterLsnIsAtOrBeyondLastIndexedLsn)
    {
        TLogPageIndex map;
        map.InitLastIndexedLsn(10);

        auto record = MakePageRecord(10, 20, {{1, Range(100, 1)}});
        UNIT_ASSERT(map.TryApplyNext(*record));

        UNIT_ASSERT(map.Lookup({Range(1, 1)}, 20).Mappings.empty());
        UNIT_ASSERT(map.Lookup({Range(1, 1)}, 100).Mappings.empty());
    }

    Y_UNIT_TEST(ShouldRemoveEverythingUpToTheGivenLsn)
    {
        TLogPageIndex map;
        map.InitLastIndexedLsn(10);

        auto first = MakePageRecord(10, 20, {{1, Range(100, 2)}});
        auto second = MakePageRecord(20, 30, {{5, Range(200, 2)}});
        auto third = MakePageRecord(30, 40, {{9, Range(300, 2)}});

        UNIT_ASSERT(map.TryApplyNext(*first));
        UNIT_ASSERT(map.TryApplyNext(*second));
        UNIT_ASSERT(map.TryApplyNext(*third));

        map.EraseUpTo(30);

        // everything at or below lsn 30 is gone, lsn 40 stays
        UNIT_ASSERT_VALUES_EQUAL("9->300x2", DescribeAll(map));

        // removing does not rewind the applied position
        UNIT_ASSERT_VALUES_EQUAL(40, map.GetLastIndexedLsn());
    }

    Y_UNIT_TEST(ShouldRemoveEverythingWhenTrimmingPastLastIndexedLsn)
    {
        TLogPageIndex map;
        map.InitLastIndexedLsn(10);

        auto record = MakePageRecord(10, 20, {{1, Range(100, 2)}});
        UNIT_ASSERT(map.TryApplyNext(*record));

        // trimming beyond the applied position drops everything applied so far
        map.EraseUpTo(30);
        UNIT_ASSERT_VALUES_EQUAL("", DescribeAll(map));

        // and it does not rewind the applied position
        UNIT_ASSERT_VALUES_EQUAL(20, map.GetLastIndexedLsn());
    }

    Y_UNIT_TEST(ShouldRemoveNothingBelowTheOldestLsn)
    {
        TLogPageIndex map;
        map.InitLastIndexedLsn(10);

        auto record = MakePageRecord(10, 20, {{1, Range(100, 2)}});
        UNIT_ASSERT(map.TryApplyNext(*record));

        map.EraseUpTo(19);
        UNIT_ASSERT_VALUES_EQUAL("1->100x2", DescribeAll(map));

        map.EraseUpTo(20);
        UNIT_ASSERT_VALUES_EQUAL("", DescribeAll(map));
    }

    Y_UNIT_TEST(ShouldBeReadableThroughAConstReference)
    {
        TLogPageIndex map;
        map.InitLastIndexedLsn(10);

        auto record = MakePageRecord(10, 20, {{1, Range(100, 2)}});
        UNIT_ASSERT(map.TryApplyNext(*record));

        const auto& constMap = map;
        UNIT_ASSERT_VALUES_EQUAL(20, constMap.GetLastIndexedLsn());
        UNIT_ASSERT_VALUES_EQUAL(
            "1->100x2",
            Describe(constMap.Lookup({Range(1, 2)}, 0).Mappings));
    }
}

////////////////////////////////////////////////////////////////////////////////

namespace {

// brute-force model: one entry per device page, no ranges at all
using TModel = TMap<ui64, std::pair<ui64 /*lsn*/, ui64 /*storePage*/>>;

void ModelApply(TModel& model, ui64 lsn, const TVector<TPageMapping>& mappings)
{
    for (const auto& [pageNo, location]: mappings) {
        for (ui64 i = 0; i < location.PageCount; ++i) {
            model[pageNo + i] = {lsn, location.FirstPageNo + i};
        }
    }
}

void ModelEraseUpTo(TModel& model, ui64 lsn)
{
    for (auto it = model.begin(); it != model.end();) {
        it = it->second.first <= lsn ? model.erase(it) : std::next(it);
    }
}

TVector<std::pair<ui64, ui64>>
ModelGet(const TModel& model, ui64 from, ui64 to, ui64 afterLsn)
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
TVector<std::pair<ui64, ui64>> Expand(const TVector<TPageMapping>& mappings)
{
    TVector<std::pair<ui64, ui64>> out;
    for (const auto& [pageNo, location]: mappings) {
        for (ui64 i = 0; i < location.PageCount; ++i) {
            out.emplace_back(pageNo + i, location.FirstPageNo + i);
        }
    }
    return out;
}

TString Show(const TVector<std::pair<ui64, ui64>>& v)
{
    TStringBuilder sb;
    for (const auto& [a, b]: v) {
        if (sb) {
            sb << " ";
        }
        sb << a << "->" << b;
    }
    return sb;
}

struct TRng
{
    ui64 S = 88172645463325252ull;

    ui64 Next(ui64 n)
    {
        S ^= S << 13;
        S ^= S >> 7;
        S ^= S << 17;
        return S % n;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLogPageIndexModelTest)
{
    Y_UNIT_TEST(DifferentialAgainstPerPageModel)
    {
        constexpr ui64 PageSpace = 48;
        ui64 mismatches = 0;

        for (ui64 seed = 1; seed <= 500 && mismatches < 5; ++seed) {
            TRng rng{seed * 6364136223846793005ull + 1442695040888963407ull};

            TLogPageIndex map;
            TModel model;
            ui64 lsn = 0;
            ui64 storeNext = 1000;

            for (ui64 step = 0; step < 40 && mismatches < 5; ++step) {
                // apply one record made of 1..3 page groups
                TVector<TPageMapping> mappings;
                const ui64 groupCnt = 1 + rng.Next(3);
                for (ui64 g = 0; g < groupCnt; ++g) {
                    const ui64 len = 1 + rng.Next(8);
                    const ui64 start = rng.Next(PageSpace - len);
                    mappings.push_back(
                        TPageMapping{
                            .PageNo = start,
                            .Location = TPageRange{
                                .FirstPageNo = storeNext,
                                .PageCount = len}});
                    storeNext += len;
                }

                auto record = std::make_shared<TLogRecord>();
                record->PrevLsn = lsn;
                record->Lsn = lsn + 10;
                record->PageMappings = mappings;
                UNIT_ASSERT(map.TryApplyNext(*record));
                lsn += 10;
                ModelApply(model, lsn, mappings);

                if (rng.Next(6) == 0) {
                    const ui64 upTo = rng.Next(lsn + 20);
                    map.EraseUpTo(upTo);
                    ModelEraseUpTo(model, upTo);
                }

                // query random windows with random afterLsn
                for (ui64 q = 0; q < 6; ++q) {
                    const ui64 len = 1 + rng.Next(PageSpace);
                    const ui64 from = rng.Next(PageSpace + 8);
                    const ui64 afterLsn = rng.Next(lsn + 20);

                    const auto got = Expand(map.Lookup(
                                                   {TPageRange{
                                                       .FirstPageNo = from,
                                                       .PageCount = len}},
                                                   afterLsn)
                                                .Mappings);
                    const auto want =
                        ModelGet(model, from, from + len, afterLsn);

                    if (Show(got) != Show(want)) {
                        ++mismatches;
                        Cerr << "\nMISMATCH seed=" << seed << " step=" << step
                             << " query=[" << from << "," << (from + len)
                             << ") afterLsn=" << afterLsn
                             << " lastIndexedLsn=" << map.GetLastIndexedLsn()
                             << "\n"
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
