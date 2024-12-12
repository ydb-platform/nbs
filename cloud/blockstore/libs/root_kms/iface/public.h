#pragma once

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IRootKmsClient;
using IRootKmsClientPtr = std::shared_ptr<IRootKmsClient>;

}   // namespace NCloud::NBlockStore
