#pragma once

#include <util/datetime/base.h>

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WAIT_TIMEOUT = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

struct TOptions;
using TOptionsPtr = std::shared_ptr<TOptions>;

struct IRunnable;
using IRunnablePtr = std::shared_ptr<IRunnable>;

struct IStorage;
using IStoragePtr = std::shared_ptr<IStorage>;

using TStorageBuffer = std::shared_ptr<char>;

}   // namespace NCloud::NBlockStore
