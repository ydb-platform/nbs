#pragma once

#include <memory>

namespace NCloud::NFileStore::NClient {

////////////////////////////////////////////////////////////////////////////////

class TClientConfig;
using TClientConfigPtr = std::shared_ptr<TClientConfig>;

struct IRetryPolicy;
using IRetryPolicyPtr = std::shared_ptr<IRetryPolicy>;

struct ISession;
using ISessionPtr = std::shared_ptr<ISession>;

struct ISessionWithIntrospection;
using ISessionWithIntrospectionPtr = std::shared_ptr<ISessionWithIntrospection>;

class TSessionConfig;
using TSessionConfigPtr = std::shared_ptr<TSessionConfig>;

}   // namespace NCloud::NFileStore::NClient
