#include "key_buffer_store.h"

#include <util/generic/map.h>
#include <util/string/builder.h>
#include <util/system/spinlock.h>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TInMemoryKeyBufferStore final: public IKeyBufferStore
{
private:
    mutable TAdaptiveLock Lock;
    TMap<ui64, TString> Buffers;

public:
    TFuture<NCloud::NProto::TError> Write(ui64 key, TString buffer) override
    {
        with_lock (Lock) {
            Buffers[key] = std::move(buffer);
        }
        return MakeFuture(MakeError(S_OK));
    }

    TFuture<TResultOrError<TString>> Read(ui64 key) const override
    {
        with_lock (Lock) {
            auto it = Buffers.find(key);
            if (it == Buffers.end()) {
                return MakeFuture<TResultOrError<TString>>(
                    MakeError(E_NOT_FOUND, TStringBuilder()
                        << "no buffer for key " << key));
            }

            return MakeFuture<TResultOrError<TString>>(it->second);
        }
    }

    TFuture<NCloud::NProto::TError> EraseUpTo(ui64 lastKey) override
    {
        with_lock (Lock) {
            auto end = Buffers.upper_bound(lastKey);
            if (end == Buffers.begin()) {
                return MakeFuture(MakeError(S_FALSE));
            }

            Buffers.erase(Buffers.begin(), end);
            return MakeFuture(MakeError(S_OK));
        }
    }

    TSet<ui64> GetKeys() const override
    {
        TSet<ui64> keys;
        with_lock (Lock) {
            for (const auto& [key, buffer]: Buffers) {
                keys.insert(key);
            }
        }
        return keys;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IKeyBufferStorePtr CreateInMemoryKeyBufferStore()
{
    return std::make_shared<TInMemoryKeyBufferStore>();
}

}   // namespace NCloud::NJournalled
