#pragma once

#include "notify.h"

namespace NCloud::NBlockStore::NNotify {

////////////////////////////////////////////////////////////////////////////////

struct TJsonGenerator final: public IJsonGenerator
{
    NJson::TJsonMap Generate(const TNotification& data) override;
};

}   // namespace NCloud::NBlockStore::NNotify
