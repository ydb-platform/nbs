#include "journalled_device.h"

#include "device.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestDevice final: public IDevice
{
    ui32 WritePagesCount = 0;

    [[nodiscard]] auto ReadPages(
        NCloud::NProto::TReadPagesRequest request)
        -> TFuture<NCloud::NProto::TReadPagesResponse> final
    {
        Y_UNUSED(request);

        return MakeFuture<NCloud::NProto::TReadPagesResponse>();
    }

    [[nodiscard]] auto WritePages(
        NCloud::NProto::TWriteLogRecordRequest request)
        -> TFuture<NCloud::NProto::TWriteLogRecordResponse> final
    {
        Y_UNUSED(request);

        ++WritePagesCount;

        return MakeFuture<NCloud::NProto::TWriteLogRecordResponse>();
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
        Device = CreateJournalledDevice(DataStore);
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

    Y_UNIT_TEST_F(ShouldValidateLogSequenceNumber, TFixture)
    {
        // the very first record is accepted with any prev lsn

        {
            const auto error = WriteLogRecord(10, 5);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        {
            const auto error = WriteLogRecord(11, 10);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }

        // a gap in the log

        {
            const auto error = WriteLogRecord(20, 15);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_STRING_CONTAINS(
                error.GetMessage(),
                "Wrong lsn: 15, expected 11");
        }

        // an outdated record

        {
            const auto error = WriteLogRecord(13, 5);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_INVALID_STATE,
                error.GetCode(),
                FormatError(error));
            UNIT_ASSERT_STRING_CONTAINS(
                error.GetMessage(),
                "Wrong lsn: 5, expected 11");
        }

        // the rejected records have not reached the data store

        UNIT_ASSERT_VALUES_EQUAL(2, DataStore->WritePagesCount);

        // the rejected records have not changed the state

        {
            const auto error = WriteLogRecord(12, 11);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                error.GetCode(),
                FormatError(error));
        }
    }
}

}   // namespace NCloud::NJournalled
