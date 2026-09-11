#include "key_buffer_store.h"

#include <util/generic/map.h>
#include <util/string/builder.h>

#include <utility>

namespace NCloud::NJournalled {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TRestoreResult = TResultOrError<TKeyBuffers>;

////////////////////////////////////////////////////////////////////////////////

class TInMemoryKeyBufferStore final: public IKeyBufferStore
{
private:
    TAdaptiveLock Lock;
    TMap<ui64, TBuffer> Buffers;
    ui64 ErasedBelowKey = 0;

public:
    TFuture<TRestoreResult> Restore() override
    {
        with_lock (Lock) {
            TKeyBuffers buffers;
            buffers.reserve(Buffers.size());
            for (const auto& [key, buffer]: Buffers) {
                buffers.emplace_back(key, buffer);
            }
            return MakeFuture<TRestoreResult>(std::move(buffers));
        }
    }

    TFuture<NCloud::NProto::TError> Write(ui64 key, TBuffer buffer) override
    {
        with_lock (Lock) {
            if (key < ErasedBelowKey) {
                return MakeFuture(MakeError(
                    E_ARGUMENT,
                    TStringBuilder() << "key " << key << " is erased"));
            }

            Buffers[key] = std::move(buffer);
        }
        return MakeFuture(MakeError(S_OK));
    }

    TFuture<NCloud::NProto::TError> EraseBelow(ui64 key) override
    {
        with_lock (Lock) {
            if (ErasedBelowKey < key) {
                ErasedBelowKey = key;
            }

            auto end = Buffers.lower_bound(key);
            if (end == Buffers.begin()) {
                return MakeFuture(MakeError(S_FALSE));
            }

            Buffers.erase(Buffers.begin(), end);
            return MakeFuture(MakeError(S_OK));
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IKeyBufferStorePtr CreateInMemoryKeyBufferStore()
{
    return std::make_shared<TInMemoryKeyBufferStore>();
}

}   // namespace NCloud::NJournalled
