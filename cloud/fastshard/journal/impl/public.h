#pragma once

#include <memory>

namespace NCloud::NJournalled {

////////////////////////////////////////////////////////////////////////////////

struct IDevicePageStore;
using IDevicePageStorePtr = std::shared_ptr<IDevicePageStore>;

struct TLogRecord;
using TLogRecordPtr = std::shared_ptr<TLogRecord>;

struct IJournal;
using IJournalPtr = std::shared_ptr<IJournal>;

struct IKeyBufferStore;
using IKeyBufferStorePtr = std::shared_ptr<IKeyBufferStore>;

}   // namespace NCloud::NJournalled
