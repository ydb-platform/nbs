#pragma once

#include "notify.h"

namespace NCloud::NBlockStore::NNotify {

////////////////////////////////////////////////////////////////////////////////

class TJsonGenerator final: public IJsonGenerator
{
public:
    TJsonGenerator() = default;

    NJson::TJsonMap Generate(const TNotification& data) override;
};

}   // namespace NCloud::NBlockStore::NNotify
