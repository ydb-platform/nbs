#pragma once

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct ICheckpointValidator;
using TValidatorPtr = std::shared_ptr<ICheckpointValidator>;

}   // namespace NCloud::NBlockStore
