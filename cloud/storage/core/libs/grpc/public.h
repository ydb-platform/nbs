#pragma once

#include <memory>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct ICertificateRefresher;
using ICertificateRefresherPtr = std::shared_ptr<ICertificateRefresher>;

struct ICertificateProvider;
using ICertificateProviderPtr = std::shared_ptr<ICertificateProvider>;

}   // namespace NCloud
