#include "block_digest.h"

#include <cloud/storage/core/libs/common/block_data_ref.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockDigestGeneratorTest)
{
    Y_UNIT_TEST(TestExt4)
    {
        auto gen = CreateExt4BlockDigestGenerator(1);
        auto offset = 256;

        UNIT_ASSERT(!gen->ComputeDigest(0, TBlockDataRef::CreateZeroBlock(2_KB)));
        UNIT_ASSERT(!gen->ComputeDigestForce(TBlockDataRef::CreateZeroBlock(2_KB)));
        UNIT_ASSERT(!gen->ComputeDigest(0, TBlockDataRef::CreateZeroBlock(256_KB)));
        UNIT_ASSERT(!gen->ComputeDigestForce(TBlockDataRef::CreateZeroBlock(256_KB)));
        UNIT_ASSERT(!gen->ComputeDigest(0, TBlockDataRef::CreateZeroBlock(5_KB)));
        UNIT_ASSERT(!gen->ComputeDigestForce(TBlockDataRef::CreateZeroBlock(5_KB)));
        UNIT_ASSERT(gen->ComputeDigest(0, TBlockDataRef::CreateZeroBlock(4_KB)));
        UNIT_ASSERT(gen->ComputeDigestForce(TBlockDataRef::CreateZeroBlock(4_KB)));
        UNIT_ASSERT(gen->ComputeDigest(0, TBlockDataRef::CreateZeroBlock(128_KB)));
        UNIT_ASSERT(gen->ComputeDigestForce(TBlockDataRef::CreateZeroBlock(128_KB)));

        TString str("asd");
        str.resize(4_KB);
        TBlockDataRef blockData(str.data(), str.size());
        UNIT_ASSERT(gen->ShouldProcess(0, 1, 4_KB));
        UNIT_ASSERT(gen->ComputeDigest(0, blockData));
        UNIT_ASSERT(gen->ShouldProcess(offset + 300, 1, 4_KB));
        UNIT_ASSERT(gen->ComputeDigest(offset + 300, blockData));
        UNIT_ASSERT(!gen->ShouldProcess(offset + 400, 1, 4_KB));
        UNIT_ASSERT(!gen->ComputeDigest(offset + 400, blockData));
        UNIT_ASSERT(gen->ComputeDigestForce(blockData));
        UNIT_ASSERT(!gen->ShouldProcess(offset + 20000, 1, 4_KB));
        UNIT_ASSERT(!gen->ComputeDigest(offset + 20000, blockData));
        UNIT_ASSERT(gen->ComputeDigestForce(blockData));

        offset += 128_MB / 4_KB;
        UNIT_ASSERT(gen->ShouldProcess(offset, 1, 4_KB));
        UNIT_ASSERT(gen->ComputeDigest(offset, blockData));
        UNIT_ASSERT(gen->ShouldProcess(offset + 300, 1, 4_KB));
        UNIT_ASSERT(gen->ComputeDigest(offset + 300, blockData));
        UNIT_ASSERT(!gen->ShouldProcess(offset + 400, 1, 4_KB));
        UNIT_ASSERT(!gen->ComputeDigest(offset + 400, blockData));
        UNIT_ASSERT(gen->ComputeDigestForce(blockData));
        UNIT_ASSERT(!gen->ShouldProcess(offset + 20000, 1, 4_KB));
        UNIT_ASSERT(!gen->ComputeDigest(offset + 20000, blockData));
        UNIT_ASSERT(gen->ComputeDigestForce(blockData));
    }

    Y_UNIT_TEST(TestTest)
    {
        auto gen = CreateTestBlockDigestGenerator();

        ui64 x = 100;
        const char* bytes = reinterpret_cast<const char*>(&x);

        // checking that when we write a ui64 into the first 8 bytes of the
        // block our digest is equal to this ui64
        auto check = [&] () {
            TString str(bytes, sizeof(x));
            str.resize(4_KB);
            TBlockDataRef blockData(str.data(), str.size());
            auto digest = gen->ComputeDigest(0, blockData);
            UNIT_ASSERT(digest.Defined());
            UNIT_ASSERT_VALUES_EQUAL(x, *digest);
        };

        check();

        x = Max<ui32>();
        check();

        // zero block
        auto digest =
            gen->ComputeDigest(0, TBlockDataRef::CreateZeroBlock(4_KB));
        UNIT_ASSERT(digest.Defined());
        UNIT_ASSERT_VALUES_EQUAL(0, *digest);

        UNIT_ASSERT(gen->ShouldProcess(0, 10, 4_KB));
        UNIT_ASSERT(gen->ShouldProcess(100500, 10, 4_KB));
    }

    Y_UNIT_TEST(TestStub)
    {
        auto gen = CreateBlockDigestGeneratorStub();

        TString str("asd");
        str.resize(4_KB);
        TBlockDataRef blockData(str.data(), str.size());
        UNIT_ASSERT(!gen->ComputeDigest(0, blockData));
        UNIT_ASSERT(!gen->ComputeDigest(512, blockData));
        UNIT_ASSERT(!gen->ComputeDigest(513, blockData));
        UNIT_ASSERT(!gen->ComputeDigest(20000, blockData));

        UNIT_ASSERT(!gen->ShouldProcess(0, 10, 4_KB));
        UNIT_ASSERT(!gen->ShouldProcess(100500, 10, 4_KB));
    }

    Y_UNIT_TEST(TestComputeDefaultDigest)
    {
        UNIT_ASSERT_VALUES_EQUAL(3387363646, ComputeDefaultDigest({"vasya", 5}));
    }
}

}   // namespace NCloud::NBlockStore
