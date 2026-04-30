#pragma once

#include "priority.h"

#include <util/generic/noncopyable.h>

#include <cstddef>
#include <memory>

struct IGlobalLogsStorage;
struct TLogRecord;

// NOTE: be aware that all `TLogBackend`s are registred in singleton.
class TLogBackend: public TNonCopyable {
private:
    std::shared_ptr<IGlobalLogsStorage> GlobalLogsStorage;

public:
    TLogBackend() noexcept;
    virtual ~TLogBackend();

    virtual void WriteData(const TLogRecord& rec) = 0;
    virtual void ReopenLog() = 0;

    // Does not guarantee consistency with previous WriteData() calls:
    // log entries could be written to the new (reopened) log file due to
    // buffering effects.
    virtual void ReopenLogNoFlush();

    virtual ELogPriority FiltrationLevel() const;

    static void ReopenAllBackends(bool flush = true);

    virtual size_t QueueSize() const;
};

struct IGlobalLogsStorage
{
    virtual ~IGlobalLogsStorage() = default;

    virtual void Register(TLogBackend* backend) = 0;
    virtual void UnRegister(TLogBackend* backend) = 0;
    virtual void Reopen(bool flush) = 0;
};
