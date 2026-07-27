#include "block_digest_factory.h"

#include "config.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBlockDigestGeneratorFactory final: IBlockDigestGeneratorFactory
{
    [[nodiscard]] auto CreateBlockDigestGenerator(const TStorageConfig& config)
        -> IBlockDigestGeneratorPtr final
    {
        if (!config.GetBlockDigestsEnabled()) {
            return CreateBlockDigestGeneratorStub();
        }

        if (config.GetUseTestBlockDigestGenerator()) {
            return CreateTestBlockDigestGenerator();
        }

        return CreateExt4BlockDigestGenerator(
            config.GetDigestedBlocksPercentage());
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBlockDigestGeneratorFactoryPtr CreateBlockDigestGeneratorFactory()
{
    return std::make_shared<TBlockDigestGeneratorFactory>();
}

}   // namespace NCloud::NBlockStore::NStorage
