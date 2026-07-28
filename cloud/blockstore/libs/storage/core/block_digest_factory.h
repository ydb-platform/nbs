#pragma once
#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct IBlockDigestGeneratorFactory
{
    virtual ~IBlockDigestGeneratorFactory() = default;

    [[nodiscard]] virtual auto CreateBlockDigestGenerator(
        const TStorageConfig& config) -> IBlockDigestGeneratorPtr = 0;
};

////////////////////////////////////////////////////////////////////////////////

IBlockDigestGeneratorFactoryPtr CreateBlockDigestGeneratorFactory();

}   // namespace NCloud::NBlockStore::NStorage
