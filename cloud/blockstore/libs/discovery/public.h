#pragma once

#include <memory>

namespace NCloud::NBlockStore {

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

class TDiscoverInstancesRequest;
class TDiscoverInstancesResponse;

class TIOCounters;

}   // namespace NProto

namespace NDiscovery {

////////////////////////////////////////////////////////////////////////////////

class TDiscoveryConfig;
using TDiscoveryConfigPtr = std::shared_ptr<TDiscoveryConfig>;

struct IDiscoveryService;
using IDiscoveryServicePtr = std::shared_ptr<IDiscoveryService>;

struct IInstanceFetcher;
using IInstanceFetcherPtr = std::shared_ptr<IInstanceFetcher>;

struct IBanList;
using IBanListPtr = std::shared_ptr<IBanList>;

struct IHealthChecker;
using IHealthCheckerPtr = std::shared_ptr<IHealthChecker>;

struct IPingClient;
using IPingClientPtr = std::shared_ptr<IPingClient>;

struct IBalancingPolicy;
using IBalancingPolicyPtr = std::shared_ptr<IBalancingPolicy>;

}   // namespace NDiscovery
}   // namespace NCloud::NBlockStore
