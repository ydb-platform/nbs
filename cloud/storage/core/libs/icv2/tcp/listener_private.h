#pragma once

#include "listener.h"

#include <functional>

namespace NCloud::NIcV2::NPrivate {

// Return an errno to inject, or zero to accept normally.
using TAcceptErrorInjector = std::function<int()>;

// For tests that need a specific backlog or injected accept errors.
TResultOrError<IListenerPtr> CreateListenerWithAcceptError(
    const NInterconnect::TAddress& address,
    int backlog,
    IListenerClientPtr client,
    TAcceptErrorInjector injectAcceptError = {});

}   // namespace NCloud::NIcV2::NPrivate
