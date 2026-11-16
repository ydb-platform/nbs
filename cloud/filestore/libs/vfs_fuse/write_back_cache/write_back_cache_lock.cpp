#include "write_back_cache_lock.h"

#include <util/system/spinlock.h>
#include <util/system/yassert.h>

#include <mutex>
#include <pthread.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TLock, unsigned SpinCount = 0>
class alignas(64) TWriteBackCacheLock final: public IWriteBackCacheLock
{
private:
    TLock Lock;

public:
    void Acquire() noexcept override
    {
        for (unsigned i = 0; i < SpinCount; ++i) {
            if (Lock.try_lock()) {
                return;
            }
            SpinLockPause();
        }

        // Lock acquisition failures are fatal: noexcept intentionally turns
        // a std::mutex::lock() exception into process termination.
        Lock.lock();
    }

    void Release() noexcept override
    {
        Lock.unlock();
    }
};

class alignas(64) TPthreadAdaptiveLock final: public IWriteBackCacheLock
{
private:
    pthread_mutex_t Mutex = PTHREAD_ADAPTIVE_MUTEX_INITIALIZER_NP;

public:
    ~TPthreadAdaptiveLock() override
    {
        const int error = pthread_mutex_destroy(&Mutex);
        Y_ABORT_UNLESS(error == 0, "pthread_mutex_destroy failed: %d", error);
    }

    void Acquire() noexcept override
    {
        const int error = pthread_mutex_lock(&Mutex);
        Y_ABORT_UNLESS(error == 0, "pthread_mutex_lock failed: %d", error);
    }

    void Release() noexcept override
    {
        const int error = pthread_mutex_unlock(&Mutex);
        Y_ABORT_UNLESS(error == 0, "pthread_mutex_unlock failed: %d", error);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IWriteBackCacheLock> CreateWriteBackCacheLock(
    NProto::EWriteBackCacheLockMethod method)
{
    switch (method) {
        case NProto::WBC_LOCK_ADAPTIVE:
            return std::make_unique<TWriteBackCacheLock<TAdaptiveLock>>();

        case NProto::WBC_LOCK_SPIN:
            return std::make_unique<TWriteBackCacheLock<TSpinLock>>();

        case NProto::WBC_LOCK_MUTEX:
            // Use std::mutex for blocking modes: TMutex is recursive and would
            // allow accidental re-entry into a critical section.
            return std::make_unique<TWriteBackCacheLock<std::mutex>>();

        case NProto::WBC_LOCK_SPIN_THEN_MUTEX:
            // Initial spin budget. Repeated try_lock calls may cause atomic
            // contention and can overtake threads already waiting on the mutex.
            return std::make_unique<TWriteBackCacheLock<std::mutex, 256>>();

        case NProto::WBC_LOCK_PTHREAD_ADAPTIVE:
            return std::make_unique<TPthreadAdaptiveLock>();
    }

    Y_ABORT("Unknown WriteBackCache lock method: %d", static_cast<int>(method));
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
