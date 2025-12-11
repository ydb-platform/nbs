#include "throttler_policy.h"

#include <cloud/blockstore/libs/throttling/throttler_policy.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceThrottlerPolicyTest)
{
    Y_UNIT_TEST(ShouldThrottlingServicePolicyRecalculateParametersCorrectly)
    {
        TThrottlingServiceConfig config(
            50_MB,                  // maxReadBandwidth
            40_MB,                  // maxWriteBandwidth
            50,                     // maxReadIops
            40,                     // maxWriteIops
            TDuration::Seconds(1)   // maxBurstTime
        );
        auto policy = CreateServiceThrottlerPolicy(config);

#define DO_TEST(expectedDelayMcs, timeMcs, requestType, bs) \
    UNIT_ASSERT_VALUES_EQUAL(                               \
        TDuration::MicroSeconds(expectedDelayMcs),          \
        policy->SuggestDelay(                               \
            TInstant::MicroSeconds(timeMcs),                \
            NCloud::NProto::STORAGE_MEDIA_DEFAULT,          \
            requestType,                                    \
            bs));                                           \
    // DO_TEST

        for (ui32 i = 0; i < 10; ++i) {
            DO_TEST(0, 10'000, EBlockStoreRequest::ReadBlocks, 4_MB);
        }
        DO_TEST(100'000, 10'000, EBlockStoreRequest::ReadBlocksLocal, 4_MB);
        DO_TEST(125'000, 10'000, EBlockStoreRequest::WriteBlocks, 4_MB);
        DO_TEST(0, 135'000, EBlockStoreRequest::WriteBlocksLocal, 4_MB);
        DO_TEST(100'000, 135'000, EBlockStoreRequest::ReadBlocksLocal, 4_MB);
        DO_TEST(35'000, 200'000, EBlockStoreRequest::ReadBlocksLocal, 4_MB);
        DO_TEST(0, 235'000, EBlockStoreRequest::ReadBlocksLocal, 4_MB);
        for (ui32 i = 0; i < 4; ++i) {
            DO_TEST(0, 1'235'000, EBlockStoreRequest::WriteBlocksLocal, 4_MB);
            DO_TEST(0, 1'235'000, EBlockStoreRequest::WriteBlocksLocal, 4_MB);
        }
        DO_TEST(100'000, 1'235'000, EBlockStoreRequest::ReadBlocks, 4_MB);
        DO_TEST(0, 2'235'000, EBlockStoreRequest::ReadBlocksLocal, 49_MB);
        DO_TEST(20'079, 2'235'000, EBlockStoreRequest::ReadBlocksLocal, 4_KB);

#undef DO_TEST
    }
}

}   // namespace NCloud::NBlockStore
