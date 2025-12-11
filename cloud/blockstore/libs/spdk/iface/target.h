#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

struct ISpdkTarget: public IStartable
{
    virtual ~ISpdkTarget() = default;

    virtual NThreading::TFuture<void> StartAsync() = 0;
    virtual NThreading::TFuture<void> StopAsync() = 0;

    virtual ISpdkDevicePtr GetDevice(const TString& name) = 0;
};

}   // namespace NCloud::NBlockStore::NSpdk
