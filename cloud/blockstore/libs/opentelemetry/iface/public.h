#pragma once

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct ITraceServiceClient;
using ITraceServiceClientPtr = std::shared_ptr<ITraceServiceClient>;

}   // namespace NCloud::NBlockStore
