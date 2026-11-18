#pragma once

#include "listener.h"

namespace NCloud::NIcV2::NPrivate {

// Return an errno to fail this attempt, or zero to proceed.
struct IListenerErrorInjector
{
    virtual ~IListenerErrorInjector();

    virtual int BeforeAccept() = 0;
    virtual int BeforePrepareSocket() = 0;
};

using IListenerErrorInjectorPtr = std::shared_ptr<IListenerErrorInjector>;

// For tests that need a specific backlog or injected accept/setup errors.
TResultOrError<IListenerPtr> CreateListener(
    const NInterconnect::TAddress& address,
    int backlog,
    IListenerClientPtr client,
    IListenerErrorInjectorPtr errorInjector);

}   // namespace NCloud::NIcV2::NPrivate
