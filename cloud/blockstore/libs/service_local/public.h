#pragma once

#include <memory>

namespace NCloud::NBlockStore {

namespace NProto {
    class TLocalServiceConfig;
}   // namespace NProto

namespace NServer {

////////////////////////////////////////////////////////////////////////////////

struct IFileIOServiceProvider;
using IFileIOServiceProviderPtr = std::shared_ptr<IFileIOServiceProvider>;

}   // namespace NServer

}   // namespace NCloud::NBlockStore
