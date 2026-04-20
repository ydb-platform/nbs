#include <cloud/blockstore/libs/service/context.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TContextTest)
{
    Y_UNIT_TEST(ShouldCastBaseCallContextToBlockStoreCallContext)
    {
        auto concrete = CreateCallContext(42);
        auto* raw = concrete.Get();

        TCallContextBasePtr base = concrete;
        UNIT_ASSERT_VALUES_EQUAL(concrete.RefCount(), 2);

        concrete.Reset();
        UNIT_ASSERT_VALUES_EQUAL(base.RefCount(), 1);

        auto restored = ToBlockStoreCallContext(std::move(base));

        UNIT_ASSERT(!base);
        UNIT_ASSERT(restored);
        UNIT_ASSERT_VALUES_EQUAL(restored.Get(), raw);
        UNIT_ASSERT_VALUES_EQUAL(restored.RefCount(), 1);
    }
}

}   // namespace NCloud::NBlockStore
