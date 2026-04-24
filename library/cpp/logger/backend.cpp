#include "backend.h"

#include <util/generic/singleton.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/system/mutex.h>

namespace {

////////////////////////////////////////////////////////////////////////////////

class TGlobalLogsStorage
{
private:
    class TImpl: public IGlobalLogsStorage
    {
    private:
        TVector<TLogBackend*> Backends;
        TMutex Mutex;

    public:
        void Register(TLogBackend* backend) override
        {
            TGuard<TMutex> g(Mutex);
            Backends.push_back(backend);
        }

        void UnRegister(TLogBackend* backend) override
        {
            TGuard<TMutex> g(Mutex);
            for (ui32 i = 0; i < Backends.size(); ++i) {
                if (Backends[i] == backend) {
                    Backends.erase(Backends.begin() + i);
                    return;
                }
            }
            Y_ABORT("Incorrect pointer for log backend");
        }

        void Reopen(bool flush) override
        {
            TGuard<TMutex> g(Mutex);
            for (auto& b: Backends) {
                if (typeid(*b) == typeid(TLogBackend)) {
                    continue;
                }
                if (flush) {
                    b->ReopenLog();
                } else {
                    b->ReopenLogNoFlush();
                }
            }
        }
    };

    std::shared_ptr<TImpl> Storage = std::make_shared<TImpl>();

public:
    std::shared_ptr<IGlobalLogsStorage> Get() const
    {
        return Storage;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ELogPriority TLogBackend::FiltrationLevel() const
{
    return LOG_MAX_PRIORITY;
}

TLogBackend::TLogBackend() noexcept
    : GlobalLogsStorage(Singleton<TGlobalLogsStorage>()->Get())
{
    GlobalLogsStorage->Register(this);
}

TLogBackend::~TLogBackend()
{
    GlobalLogsStorage->UnRegister(this);
}

void TLogBackend::ReopenLogNoFlush()
{
    ReopenLog();
}

void TLogBackend::ReopenAllBackends(bool flush)
{
    Singleton<TGlobalLogsStorage>()->Get()->Reopen(flush);
}

size_t TLogBackend::QueueSize() const
{
    ythrow yexception() << "Not implemented.";
}
