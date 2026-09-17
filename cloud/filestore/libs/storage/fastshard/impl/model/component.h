#pragma once

#include "page_store.h"

#include <cloud/storage/core/libs/common/error.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

struct IComponent
{
    virtual ~IComponent() = default;

    [[nodiscard]] virtual TString Describe() const = 0;
    virtual NProto::TError CheckFormat(TWriteContext& writeContext) = 0;
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
