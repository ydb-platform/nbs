#pragma once

#include <memory>

namespace NCloud::NBlockStore {

namespace NClient {

////////////////////////////////////////////////////////////////////////////////

struct IBlockStoreValidationClient;
using IBlockStoreValidationClientPtr = std::shared_ptr<IBlockStoreValidationClient>;

}   // namespace NClient

struct IValidationCallback;
using IValidationCallbackPtr = std::shared_ptr<IValidationCallback>;

struct IBlockDigestCalculator;
using IBlockDigestCalculatorPtr = std::shared_ptr<IBlockDigestCalculator>;

}   // namespace NCloud::NBlockStore
