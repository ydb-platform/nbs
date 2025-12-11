#include "balancing.h"

#include <cloud/blockstore/public/api/protos/discovery.pb.h>

#include <util/generic/algorithm.h>

namespace NCloud::NBlockStore::NDiscovery {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TBalancingPolicy final: public IBalancingPolicy
{
public:
    void Reorder(TInstanceList& instances) override
    {
        with_lock (instances.Lock) {
            for (auto& ii: instances.Instances) {
                if (ii.Status == TInstanceInfo::EStatus::Unreachable) {
                    ii.BalancingScore = 0;
                    continue;
                }

                if (ii.LastStat) {
                    if (ii.PrevStat) {
                        const auto diff =
                            Max(1.,
                                double(ii.LastStat.Bytes) - ii.PrevStat.Bytes);
                        const auto timeDiff = ii.LastStat.Ts - ii.PrevStat.Ts;
                        ii.BalancingScore =
                            timeDiff.MicroSeconds() / (diff * 1e6);
                    } else {
                        ii.BalancingScore = 1;
                    }
                }
            }

            // stability needed for uts
            StableSortBy(
                instances.Instances.begin(),
                instances.Instances.end(),
                [](const auto& ii) { return -ii.BalancingScore; });
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IBalancingPolicyPtr CreateBalancingPolicy()
{
    return std::make_shared<TBalancingPolicy>();
}

}   // namespace NCloud::NBlockStore::NDiscovery
