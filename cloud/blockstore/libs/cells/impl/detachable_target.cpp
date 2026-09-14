#include "detachable_target.h"

#include <util/system/spinlock.h>

namespace NCloud::NBlockStore::NCells {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDetachableTarget final: public IDetachableTarget
{
private:
    TAdaptiveLock Lock;
    ITransportTargetPtr Target;

public:
    explicit TDetachableTarget(ITransportTargetPtr target)
        : Target(std::move(target))
    {}

    void SetTarget(IBlockStorePtr target) override
    {
        // under the lock, so that a detach racing this call either lets it
        // through whole or stops it entirely
        with_lock (Lock) {
            if (Target) {
                Target->SetTarget(std::move(target));
            }
        }
    }

    void Detach() override
    {
        with_lock (Lock) {
            Target.reset();
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IDetachableTargetPtr CreateDetachableTarget(ITransportTargetPtr target)
{
    return std::make_shared<TDetachableTarget>(std::move(target));
}

}   // namespace NCloud::NBlockStore::NCells
