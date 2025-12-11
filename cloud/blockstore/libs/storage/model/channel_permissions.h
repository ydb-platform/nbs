#pragma once

#include <util/generic/flags.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

enum class EChannelPermission
{
    UserWritesAllowed = (1 << 0),
    SystemWritesAllowed = (1 << 1),
};

Y_DECLARE_FLAGS(EChannelPermissions, EChannelPermission);
Y_DECLARE_OPERATORS_FOR_FLAGS(EChannelPermissions);

}   // namespace NCloud::NBlockStore::NStorage
