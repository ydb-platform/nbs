#pragma once

#include <memory>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

struct TOptions;
using TOptionsPtr = std::shared_ptr<TOptions>;

struct IRunnable;
using IRunnablePtr = std::shared_ptr<IRunnable>;

}   // namespace NCloud::NBlockStore
