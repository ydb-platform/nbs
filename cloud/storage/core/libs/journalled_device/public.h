#pragma once

#include <library/cpp/threading/future/core/fwd.h>

#include <memory>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TResultOrError;

}   // namespace NCloud

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
using TFutureResultOrError = NThreading::TFuture<TResultOrError<T>>;

struct TLogRecord;
using TLogRecordPtr = std::shared_ptr<TLogRecord>;

struct IPageStore;
using IPageStorePtr = std::shared_ptr<IPageStore>;

struct IKeyBufferStore;
using IKeyBufferStorePtr = std::shared_ptr<IKeyBufferStore>;

struct IJournalledDevice;
using IJournalledDevicePtr = std::shared_ptr<IJournalledDevice>;

}   // namespace NCloud::NJournalled
