#include "journal_store.h"

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
    TMap<ui64, TBuffer> Buffers;

public:
    TFuture<NCloud::NProto::TError> Write(ui64 key, TBuffer buffer) override
    {
        with_lock (Lock) {
            Buffers[key] = std::move(buffer);
        }
        return MakeFuture(MakeError(S_OK));
    }

    TFutureResultOrError<TBuffer> Read(ui64 key) const override
    {
        with_lock (Lock) {
            auto it = Buffers.find(key);
            if (it == Buffers.end()) {
                return MakeFuture<TResultOrError<TBuffer>>(
                    MakeError(E_NOT_FOUND, TStringBuilder()
                        << "no buffer for key " << key));
            }

            return MakeFuture<TResultOrError<TBuffer>>(it->second);
        }
    }

    TFuture<NCloud::NProto::TError> EraseTo(ui64 key) override
    {
        with_lock (Lock) {
            auto end = Buffers.upper_bound(key);
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

////////////////////////////////////////////////////////////////////////////////

class TInMemoryPageStore final: public IPageStore
{
private:
    mutable TAdaptiveLock Lock;
    TMap<ui64 /*pageNo*/, TBuffer> Pages;
    ui64 NextPageNo = 0;

    // Every ref must have been handed out by this store, so its page size has
    // to match the one the store was created with.
    NCloud::NProto::TError ValidateRefs(
        const TVector<TPageGroupRef>& refs) const
    {
        for (const auto& ref: refs) {
            for (ui64 i = 0; i < ref.PageCount; ++i) {
                const ui64 pageNo = ref.FirstPageNo + i;
                if (!Pages.contains(pageNo)) {
                    return MakeError(E_NOT_FOUND, TStringBuilder()
                        << "no page " << pageNo);
                }
            }
        }

        return MakeError(S_OK);
    }

public:
    auto WritePageGroups(const NCloud::NProto::TWriteLogRecordRequest& request)
        -> TFutureResultOrError<TVector<TPageGroupRef>> override
    {
        using TResult = TResultOrError<TVector<TPageGroupRef>>;

        with_lock (Lock) {
            TVector<TPageGroupRef> refs;
            refs.reserve(request.GetPageGroups().size());

            for (const auto& group: request.GetPageGroups()) {
                auto& ref = refs.emplace_back();
                ref.FirstPageNo = NextPageNo;
                ref.PageCount = group.ContentSize();

                for (const auto& page: group.GetContent()) {
                    Pages[NextPageNo++] = TBuffer(page.data(), page.size());
                }
            }

            return MakeFuture<TResult>(std::move(refs));
        }
    }

    auto ReadPageGroups(const TVector<TPageGroupRef>& pageGroupRefs)
        -> TFutureResultOrError<TVector<TBuffer>> override
    {
        with_lock (Lock) {
            if (auto error = ValidateRefs(pageGroupRefs); HasError(error)) {
                return MakeFuture<TResultOrError<TVector<TBuffer>>>(error);
            }

            TVector<TBuffer> buffers;
            for (const auto& ref: pageGroupRefs) {
                for (ui64 i = 0; i < ref.PageCount; ++i) {
                    buffers.push_back(Pages.at(ref.FirstPageNo + i));
                }
            }

            return MakeFuture<TResultOrError<TVector<TBuffer>>>(
                std::move(buffers));
        }
    }

    NCloud::NProto::TError Free(
        const TVector<TPageGroupRef>& pageGroupRefs) override
    {
        with_lock (Lock) {
            if (auto error = ValidateRefs(pageGroupRefs); HasError(error)) {
                return error;
            }

            for (const auto& ref: pageGroupRefs) {
                for (ui64 i = 0; i < ref.PageCount; ++i) {
                    Pages.erase(ref.FirstPageNo + i);
                }
            }

            return MakeError(S_OK);
        }
    }

    // pages are written up front in tests, so this only checks they are there
    NCloud::NProto::TError MarkAsWritten(
        const TVector<TPageGroupRef>& pageGroupRefs) override
    {
        with_lock (Lock) {
            return ValidateRefs(pageGroupRefs);
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IKeyBufferStorePtr CreateInMemoryKeyBufferStore()
{
    return std::make_shared<TInMemoryKeyBufferStore>();
}

IPageStorePtr CreateInMemoryPageStore()
{
    return std::make_shared<TInMemoryPageStore>();
}

}   // namespace NCloud::NJournalled
