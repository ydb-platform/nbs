#include "journalled_device.h"

#include "journal_store.h"

#include <library/cpp/testing/unittest/registar.h>

#include <thread>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

// Stands in for the real device: a flat page store that answers whatever it was
// seeded with and records what it was asked for.
struct TFakeDevice final: public IDevice
{
    THashMap<ui64, TString> Pages;
    TVector<TString> Requests;
    bool Fail = false;
    NThreading::TPromise<void> ReadGate;
    // a device that answers more than it was asked for
    ui64 ExtraPagesPerRange = 0;

    auto ReadPages(TInstant now, NCloud::NProto::TReadPagesRequest request)
        -> NThreading::TFuture<NCloud::NProto::TReadPagesResponse> override
    {
        Y_UNUSED(now);

        TStringBuilder asked;
        for (const auto& ref: request.GetPageGroupRefs()) {
            if (asked) {
                asked << ",";
            }
            asked << ref.GetFirstPageNo() << "x" << ref.GetPageCount();
        }
        Requests.push_back(asked);

        if (ReadGate.Initialized()) {
            ReadGate.GetFuture().Wait();
        }

        NCloud::NProto::TReadPagesResponse response;
        if (Fail) {
            *response.MutableError() = MakeError(E_IO, "device is down");
            return NThreading::MakeFuture(response);
        }

        for (const auto& ref: request.GetPageGroupRefs()) {
            auto& group = *response.AddPageGroups();
            group.SetFirstPageNo(ref.GetFirstPageNo());
            const ui64 pageCount = ref.GetPageCount() + ExtraPagesPerRange;
            for (ui64 i = 0; i < pageCount; ++i) {
                group.AddContent(Pages[ref.GetFirstPageNo() + i]);
            }
        }

        return NThreading::MakeFuture(response);
    }

    TVector<TString> Written;

    auto WritePages(TInstant, NCloud::NProto::TWriteLogRecordRequest request)
        -> NThreading::TFuture<NCloud::NProto::TWriteLogRecordResponse> override
    {
        TStringBuilder sb;
        sb << request.GetPrevLogSequenceNumber() << "->"
           << request.GetLogSequenceNumber() << " ";
        for (const auto& g: request.GetPageGroups()) {
            sb << g.GetFirstPageNo() << ":";
            for (const auto& c: g.GetContent()) {
                sb << "[" << c << "]";
                Pages[g.GetFirstPageNo()] = c;
            }
        }
        Written.push_back(sb);

        return NThreading::MakeFuture(
            NCloud::NProto::TWriteLogRecordResponse());
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

NCloud::NProto::TReadPagesRequest MakeRead(
    const TVector<std::pair<ui64, ui64>>& ranges)
{
    NCloud::NProto::TReadPagesRequest request;
    for (const auto& [firstPageNo, pageCount]: ranges) {
        auto& ref = *request.AddPageGroupRefs();
        ref.SetFirstPageNo(firstPageNo);
        ref.SetPageCount(pageCount);
    }
    return request;
}

TString Describe(const NCloud::NProto::TReadPagesResponse& response)
{
    TStringBuilder sb;
    for (const auto& group: response.GetPageGroups()) {
        if (sb) {
            sb << " ";
        }
        sb << group.GetFirstPageNo() << ":";
        for (const auto& page: group.GetContent()) {
            sb << "[" << page << "]";
        }
    }
    return sb;
}

struct TFixture
{
    std::shared_ptr<TFakeDevice> Device = std::make_shared<TFakeDevice>();
    IJournalledDevicePtr JournalledDevice = CreateJournalledDeviceV2(
        CreateInMemoryKeyBufferStore(),
        CreateInMemoryPageStore(),
        Device);

    void Write(ui64 prevLsn, ui64 lsn, ui64 firstPageNo, const TVector<TString>& c)
    {
        auto future = JournalledDevice->WriteLogRecord(
            TInstant::Now(),
            MakeWrite(prevLsn, lsn, firstPageNo, c));
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            future.GetValue().GetError().GetCode());
    }

    NCloud::NProto::TReadPagesResponse Read(
        const TVector<std::pair<ui64, ui64>>& ranges)
    {
        auto future = JournalledDevice->ReadPages(
            TInstant::Now(),
            MakeRead(ranges));
        UNIT_ASSERT(future.HasValue());
        return future.GetValue();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TJournalledDeviceV2Test)
{
    Y_UNIT_TEST(ShouldServeEverythingFromTheDeviceWhenTheJournalIsEmpty)
    {
        TFixture f;
        f.Device->Pages = {{10, "aaaa"}, {11, "bbbb"}};

        UNIT_ASSERT_VALUES_EQUAL("10:[aaaa][bbbb]", Describe(f.Read({{10, 2}})));
        UNIT_ASSERT_VALUES_EQUAL(1, f.Device->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL("10x2", f.Device->Requests[0]);
    }

    Y_UNIT_TEST(ShouldServeJournalledPagesWithoutTouchingTheDevice)
    {
        TFixture f;
        f.Device->Pages = {{10, "old!"}, {11, "old!"}};
        f.Write(0, 10, 10, {"aaaa", "bbbb"});

        UNIT_ASSERT_VALUES_EQUAL("10:[aaaa][bbbb]", Describe(f.Read({{10, 2}})));
        UNIT_ASSERT_C(
            f.Device->Requests.empty(),
            "the journal covered the whole request");
    }

    Y_UNIT_TEST(ShouldMergeJournalledPagesOverDevicePages)
    {
        TFixture f;
        f.Device->Pages = {
            {10, "d-10"}, {11, "d-11"}, {12, "d-12"}, {13, "d-13"}};

        // the journal holds only the middle of the requested range
        f.Write(0, 10, 11, {"j-11", "j-12"});

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[d-10][j-11][j-12][d-13]",
            Describe(f.Read({{10, 4}})));

        // and only the gaps were asked of the device
        UNIT_ASSERT_VALUES_EQUAL(1, f.Device->Requests.size());
        UNIT_ASSERT_VALUES_EQUAL("10x1,13x1", f.Device->Requests[0]);
    }

    Y_UNIT_TEST(ShouldPreferJournalledPagesOverDeviceOnes)
    {
        TFixture f;
        f.Device->Pages = {{10, "d-10"}, {11, "d-11"}, {12, "d-12"}};

        // the journal holds 11 and 12
        f.Write(0, 10, 11, {"j-11", "j-12"});

        // and the device answers past what it was asked for, so it returns
        // those pages too - the journalled versions must still win
        f.Device->ExtraPagesPerRange = 2;

        UNIT_ASSERT_VALUES_EQUAL(
            "10:[d-10][j-11][j-12]",
            Describe(f.Read({{10, 3}})));
    }

    Y_UNIT_TEST(ShouldKeepRequestOrderAcrossSeveralRanges)
    {
        TFixture f;
        f.Device->Pages = {{5, "d-5"}, {20, "d-20"}};
        f.Write(0, 10, 12, {"j-12"});

        UNIT_ASSERT_VALUES_EQUAL(
            "20:[d-20] 12:[j-12] 5:[d-5]",
            Describe(f.Read({{20, 1}, {12, 1}, {5, 1}})));
    }

    Y_UNIT_TEST(ShouldReportTheDeviceError)
    {
        TFixture f;
        f.Device->Fail = true;

        auto response = f.Read({{10, 1}});
        UNIT_ASSERT_VALUES_EQUAL(E_IO, response.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldReadTheJournalTail)
    {
        TFixture f;
        f.Write(0, 10, 100, {"aaaa"});
        f.Write(10, 20, 200, {"bbbb"});

        NCloud::NProto::TReadJournalTailRequest request;
        request.SetAfterLogSequenceNumber(0);
        request.SetMaxRecordCount(0);

        auto future =
            f.JournalledDevice->ReadJournalTail(TInstant::Now(), request);
        UNIT_ASSERT(future.HasValue());

        TStringBuilder lsns;
        for (const auto& record: future.GetValue().GetRecords()) {
            if (lsns) {
                lsns << ",";
            }
            lsns << record.GetLogSequenceNumber();
        }
        UNIT_ASSERT_VALUES_EQUAL("10,20", TString(lsns));
    }

    Y_UNIT_TEST(ShouldAdvanceTheLowWatermark)
    {
        TFixture f;
        f.Write(0, 10, 100, {"aaaa"});
        f.Write(10, 20, 200, {"bbbb"});

        NCloud::NProto::TAdvanceLsnLowWatermarkRequest request;
        request.SetLsnLowWatermark(20);

        auto future =
            f.JournalledDevice->AdvanceLsnLowWatermark(TInstant::Now(), request);
        UNIT_ASSERT(future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(
            S_OK,
            future.GetValue().GetError().GetCode());

        // the client has consumed both records, so the tail is empty now
        NCloud::NProto::TReadJournalTailRequest tailRequest;
        auto tail =
            f.JournalledDevice->ReadJournalTail(TInstant::Now(), tailRequest);
        UNIT_ASSERT_VALUES_EQUAL(0, tail.GetValue().GetRecords().size());
    }
}

}   // namespace NCloud::NJournalled
