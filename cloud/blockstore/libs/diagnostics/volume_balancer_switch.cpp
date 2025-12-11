#include "volume_balancer_switch.h"

#include <atomic>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TVolumeBalancerSwitch: public IVolumeBalancerSwitch
{
    std::atomic<bool> Enabled;

    void EnableVolumeBalancer() override
    {
        Enabled.store(true);
    }

    bool IsBalancerEnabled() override
    {
        return Enabled.load();
    }
};

////////////////////////////////////////////////////////////////////////////////

IVolumeBalancerSwitchPtr CreateVolumeBalancerSwitch()
{
    return std::make_shared<TVolumeBalancerSwitch>();
}

}   // namespace NCloud::NBlockStore
