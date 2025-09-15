#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/memory/alloc.h>

namespace NCloud::NFileStore::NSpdk {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration StartTimeout = TDuration::Seconds(30);

////////////////////////////////////////////////////////////////////////////////

struct ISpdkEnv
    : public IStartable
{
    virtual ~ISpdkEnv() = default;

    //
    // Initialization
    //

    virtual NThreading::TFuture<void> StartAsync() = 0;
    virtual NThreading::TFuture<void> StopAsync() = 0;

    //
    // Memory
    //

    virtual IAllocator* GetAllocator() = 0;
};

}   // namespace NCloud::NFileStore::NSpdk
