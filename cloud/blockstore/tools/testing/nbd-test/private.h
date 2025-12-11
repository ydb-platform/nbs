#pragma once

#include <util/datetime/base.h>

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

struct TOptions;
using TOptionsPtr = std::shared_ptr<TOptions>;

struct IRunnable;
using IRunnablePtr = std::shared_ptr<IRunnable>;

}   // namespace NCloud::NBlockStore
