#include "part_nonrepl_common.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNonreplicatedPartitionUtilTest)
{
    void DoTestShouldBuildDeviceRequests(NProto::TWriteBlocksRequest& request)
    {
        TVector<TDeviceRequest> deviceRequests;
        NProto::TDeviceConfig dummy;
        const size_t blockCountForFirstDevice = 128;
        const size_t blockCountForSecondDevice = 1024 - 128;
        deviceRequests.push_back({
            dummy,
            0,
            TBlockRange64::WithLength(100'000, blockCountForFirstDevice),
            TBlockRange64::WithLength(100, blockCountForFirstDevice)
        });
        deviceRequests.push_back({
            dummy,
            1,
            TBlockRange64::WithLength(100'128, blockCountForSecondDevice),
            TBlockRange64::WithLength(0, blockCountForSecondDevice )
        });

        TDeviceRequestBuilder builder(
            deviceRequests,
            4_KB,
            request);

        NProto::TWriteDeviceBlocksRequest r;

        builder.BuildNextRequest(r);
        UNIT_ASSERT_VALUES_EQUAL(128, r.GetBlocks().GetBuffers().size());
        for (ui32 i = 0; i < 128; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                TString(4_KB, i + 1),
                r.GetBlocks().GetBuffers()[i]);
        }

        r.Clear();
        builder.BuildNextRequest(r);
        UNIT_ASSERT_VALUES_EQUAL(1024 - 128, r.GetBlocks().GetBuffers().size());
        for (ui32 i = 0; i < 1024 - 128; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                TString(4_KB, 128 + i + 1),
                r.GetBlocks().GetBuffers()[i]);
        }
    }

    Y_UNIT_TEST(ShouldBuildDeviceRequestsForLargeRequest)
    {
        NProto::TWriteBlocksRequest request;
        request.SetStartIndex(100'000);
        auto* buffer = request.MutableBlocks()->MutableBuffers()->Add();
        buffer->resize(4_MB, 1);
        for (ui32 i = 0; i < 1024; ++i) {
            memset(buffer->begin() + i * 4_KB, i + 1, 4_KB);
        }

        DoTestShouldBuildDeviceRequests(request);
    }

    Y_UNIT_TEST(ShouldBuildDeviceRequestsForSmallRequest)
    {
        NProto::TWriteBlocksRequest request;
        request.SetStartIndex(100'000);
        for (ui32 i = 0; i < 1024; ++i) {
            auto* buffer = request.MutableBlocks()->MutableBuffers()->Add();
            buffer->resize(4_KB, 1);
            memset(buffer->begin(), i + 1, 4_KB);
        }

        DoTestShouldBuildDeviceRequests(request);
    }

    Y_UNIT_TEST(ShouldBuildDeviceRequestsWhenFirstDeviceSkiped)
    {
        NProto::TWriteBlocksRequest request;
        request.SetStartIndex(100'000 - 128);
        for (ui32 i = 0; i < 1024 + 128; ++i) {
            auto* buffer = request.MutableBlocks()->MutableBuffers()->Add();
            buffer->resize(4_KB, 1);
            memset(buffer->begin(), i + 1 - 128, 4_KB);
        }

        DoTestShouldBuildDeviceRequests(request);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
