#include "journalled_device_v1.h"

#include <cloud/fastshard/libs/journal/iface/device.h>
#include <cloud/fastshard/libs/journal/iface/journalled_device.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestDevice final: public IDevice
{
    ui32 WritePagesCount = 0;
    TVector<TPageRange> LastWrittenRanges;
    NProto::TError WritePagesError;

    [[nodiscard]] auto ReadPages(TVector<TPageRangeRef> rangeRefs)
        -> TFuture<TResultOrError<TVector<TBuffer>>> final
    {
        Y_UNUSED(rangeRefs);

        return MakeFuture<TResultOrError<TVector<TBuffer>>>(
            TVector<TBuffer>());
    }

    [[nodiscard]] auto WritePages(
        TVector<TPageRange> ranges) -> TFuture<NProto::TError> final
    {
        ++WritePagesCount;
        LastWrittenRanges = std::move(ranges);

        return MakeFuture(WritePagesError);
    }

    [[nodiscard]] auto ZeroPages(
        TVector<TPageRangeRef> ranges) -> TFuture<NProto::TError> final
    {
        Y_UNUSED(ranges);

        return MakeFuture<NProto::TError>();
    }
};

////////////////////////////////////////////////////////////////////////////////

TBuffer MakePage(char fill)
{
    TBuffer page;
    page.Fill(fill, 4096);
    return page;
}

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    std::shared_ptr<TTestDevice> DataStore;
    IJournalledDevicePtr Device;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        DataStore = std::make_shared<TTestDevice>();
        Device = CreateJournalledDeviceV1(DataStore);
    }

    NProto::TError WriteLogRecord(ui64 lsn, ui64 prevLsn)
    {
        TVector<TPageRange> ranges;
        auto& range = ranges.emplace_back();
        range.FirstPageNo = 0x10;
        range.Pages.push_back(MakePage('A'));

        return Device->WriteLogRecord(std::move(ranges), lsn, prevLsn)
            .GetValueSync();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TJournalledDeviceTest)
{
    Y_UNIT_TEST_F(ShouldRejectNonIncreasingLogSequenceNumber, TFixture)
    {
        // an unset lsn

        {
            const auto error = WriteLogRecord(0, 0);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                error.GetCode(),
                FormatError(error));
        }

        // the lsn is equal to the prev one

        {
            const auto error = WriteLogRecord(10, 10);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                error.GetCode(),
                FormatError(error));
        }

        // the lsn is below the prev one

        {
            const auto error = WriteLogRecord(5, 10);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                error.GetCode(),
                FormatError(error));
        }

        // the rejected records have not reached the data store

        UNIT_ASSERT_VALUES_EQUAL(0, DataStore->WritePagesCount);
    }

    Y_UNIT_TEST_F(ShouldWritePagesToDataStore, TFixture)
    {
        TVector<TPageRange> ranges(1);
        ranges[0].FirstPageNo = 0x10;
        ranges[0].Pages.push_back(MakePage('A'));
        ranges[0].Pages.push_back(MakePage('B'));

        const auto error = Device->WritePages(std::move(ranges)).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), FormatError(error));

        UNIT_ASSERT_VALUES_EQUAL(1, DataStore->WritePagesCount);

        const auto& written = DataStore->LastWrittenRanges;
        UNIT_ASSERT_VALUES_EQUAL(1, written.size());
        UNIT_ASSERT_VALUES_EQUAL(0x10, written[0].FirstPageNo);
        UNIT_ASSERT_VALUES_EQUAL(2, written[0].Pages.size());

        const TString expected[] = {TString(4096, 'A'), TString(4096, 'B')};

        for (size_t i = 0; i != std::size(expected); ++i) {
            const auto& page = written[0].Pages[i];
            UNIT_ASSERT_VALUES_EQUAL_C(
                expected[i],
                TStringBuf(page.Data(), page.Size()),
                i);
        }
    }

    Y_UNIT_TEST_F(ShouldReportWritePagesError, TFixture)
    {
        DataStore->WritePagesError = MakeError(E_IO, "device is broken");

        TVector<TPageRange> ranges(1);
        ranges[0].FirstPageNo = 0x10;
        ranges[0].Pages.push_back(MakePage('A'));

        const auto error = Device->WritePages(std::move(ranges)).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(E_IO, error.GetCode(), FormatError(error));
    }
}

}   // namespace NCloud::NJournalled
