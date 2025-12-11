#pragma once

#include <memory>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

class IRequestPrinter;
using IRequestPrinterPtr = std::shared_ptr<IRequestPrinter>;

class IRequestFilter;
using IRequestFilterPtr = std::shared_ptr<IRequestFilter>;

}   // namespace NCloud::NFileStore
