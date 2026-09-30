#include "journalled_device_v1.h"

#include <cloud/fastshard/journal/iface/device.h>
#include <cloud/fastshard/journal/iface/journalled_device.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestDevice final: public IDevice
{
    ui32 WritePagesCount = 0;

    [[nodiscard]] auto ReadPages(TVector<TPageRangeRef> rangeRefs)
        -> TFuture<TResultOrError<TVector<TBuffer>>> final
    {
        Y_UNUSED(rangeRefs);

        return MakeFuture<TResultOrError<TVector<TBuffer>>>(
            TVector<TBuffer>());
    }

    [[nodiscard]] auto WritePages(TVector<TPageRange> ranges)
        -> TFuture<NCloud::NProto::TError> final
    {
        Y_UNUSED(ranges);

        ++WritePagesCount;

        return MakeFuture<NCloud::NProto::TError>();
    }
};

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
        NCloud::NProto::TWriteLogRecordRequest request;
        request.SetLogSequenceNumber(lsn);
        request.SetPrevLogSequenceNumber(prevLsn);

        auto& group = *request.MutablePageGroups()->Add();
        group.SetFirstPageNo(0x10);
        group.MutableContent()->Add()->resize(4096, 'A');

        return Device->WriteLogRecord(std::move(request))
            .GetValueSync()
            .GetError();
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
            UNIT_ASSERT_STRING_CONTAINS(
                error.GetMessage(),
                "invalid lsn: 0");
        }

        // the lsn is equal to the prev one

        {
            const auto error = WriteLogRecord(10, 10);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_STRING_CONTAINS(
                error.GetMessage(),
                "invalid lsn: 10, must be greater than the prev one: 10");
        }

        // the lsn is below the prev one

        {
            const auto error = WriteLogRecord(5, 10);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_STRING_CONTAINS(
                error.GetMessage(),
                "invalid lsn: 5, must be greater than the prev one: 10");
        }

        // the rejected records have not reached the data store

        UNIT_ASSERT_VALUES_EQUAL(0, DataStore->WritePagesCount);
    }
}

}   // namespace NCloud::NJournalled
