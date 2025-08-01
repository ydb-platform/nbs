#pragma once

#include <util/system/mutex.h>

namespace NThreading {
    template <typename T, typename M = TMutex>
    class TSynchronized {
    public:
        TSynchronized(T value = {})
            : Value(std::move(value))
        {
        }

        class TAccess {
        public:
            TAccess(M& mutex, T& value)
                : Guard(mutex)
                , Value(value)
            {
            }

            T& operator*() {
                return Value;
            }

            T* operator->() {
                return &Value;
            }

        private:
            TGuard<M> Guard;
            T& Value;
        };

        TAccess Access() {
            return {Mutex, Value};
        }

        TAccess operator->() {
            return {Mutex, Value};
        }

        template <typename TFunc>
        auto Do(const TFunc& f) -> decltype(f(*Access())) {
            return f(*Access());
        }

    private:
        T Value;
        M Mutex;
    };

}
