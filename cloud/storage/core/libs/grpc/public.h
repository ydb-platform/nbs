#pragma once

#include <memory>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct ICertificateProvider;
using ICertificateProviderPtr = std::shared_ptr<ICertificateProvider>;

struct ICertificateWatcher;
using ICertificateWatcherPtr = std::shared_ptr<ICertificateWatcher>;

}   // namespace NCloud
