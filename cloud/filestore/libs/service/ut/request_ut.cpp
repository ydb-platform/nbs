#include <cloud/filestore/libs/service/request.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCalculateRequestSizeTest)
{
    Y_UNIT_TEST(ShouldReturnLengthForReadData)
    {
        NProto::TReadDataRequest request;
        request.SetLength(4096);

        UNIT_ASSERT_VALUES_EQUAL(4096, CalculateRequestSize(request));
    }

    Y_UNIT_TEST(ShouldReturnBufferSizeForWriteDataWithBuffer)
    {
        NProto::TWriteDataRequest request;
        request.SetBuffer("hello world");

        UNIT_ASSERT_VALUES_EQUAL(11, CalculateRequestSize(request));
    }

    Y_UNIT_TEST(ShouldSumIovecLengthsForWriteDataWithIovecs)
    {
        NProto::TWriteDataRequest request;

        auto* iovec1 = request.MutableIovecs()->Add();
        iovec1->SetLength(100);

        auto* iovec2 = request.MutableIovecs()->Add();
        iovec2->SetLength(200);

        auto* iovec3 = request.MutableIovecs()->Add();
        iovec3->SetLength(300);

        UNIT_ASSERT_VALUES_EQUAL(600, CalculateRequestSize(request));
    }

    Y_UNIT_TEST(ShouldReturnZeroForWriteDataWithNoBufferAndNoIovecs)
    {
        NProto::TWriteDataRequest request;

        UNIT_ASSERT_VALUES_EQUAL(0, CalculateRequestSize(request));
    }

    Y_UNIT_TEST(ShouldPreferBufferOverIovecsForWriteData)
    {
        NProto::TWriteDataRequest request;
        request.SetBuffer("hello");

        auto* iovec = request.MutableIovecs()->Add();
        iovec->SetLength(999);

        UNIT_ASSERT_VALUES_EQUAL(5, CalculateRequestSize(request));
    }

    Y_UNIT_TEST(ShouldReturnZeroForUnrelatedRequest)
    {
        NProto::TCreateNodeRequest request;

        UNIT_ASSERT_VALUES_EQUAL(0, CalculateRequestSize(request));
    }
}

}   // namespace NCloud::NFileStore
