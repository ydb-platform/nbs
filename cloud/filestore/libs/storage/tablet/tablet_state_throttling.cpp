#include "tablet_state_impl.h"

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

const TThrottlingPolicy& TIndexTabletState::GetThrottlingPolicy() const
{
    return Impl->ThrottlingPolicy;
}

TThrottlingPolicy& TIndexTabletState::AccessThrottlingPolicy()
{
    return Impl->ThrottlingPolicy;
}

const TThrottlerConfig& TIndexTabletState::GetThrottlingConfig() const
{
    return GetThrottlingPolicy().GetConfig();
}

}   // namespace NCloud::NFileStore::NStorage
