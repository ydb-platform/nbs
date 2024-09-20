#pragma once

#include <memory>

namespace NCloud::NFileStore::NReplay {

////////////////////////////////////////////////////////////////////////////////

struct TAppContext;

struct IClientFactory;
using IClientFactoryPtr = std::shared_ptr<IClientFactory>;

struct IRequestGenerator;
using IRequestGeneratorPtr = std::shared_ptr<IRequestGenerator>;

struct ITest;
using ITestPtr = std::shared_ptr<ITest>;

}   // namespace NCloud::NFileStore::NReplay
