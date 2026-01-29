#include "persistent_request_storage.h"

#include "sequence_id_generator_impl.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/overloaded.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TStorage: IPersistentStorage
{
    TMap<const void*, TString> Data;
    size_t Capacity = 0;

    bool Empty() const override
    {
        return Data.empty();
    }

    void Visit(const TVisitor& visitor) override
    {
        for (const auto& [key, value]: Data) {
            visitor(value);
        }
    }

    ui64 GetMaxSupportedAllocationByteCount() const override
    {
        return Max<ui64>();
    }

    const void* Alloc(const TAllocationWriter& writer, size_t size) override
    {
        if (Capacity && Data.size() >= Capacity) {
            return nullptr;
        }

        auto str = TString::Uninitialized(size);
        writer(str.begin(), size);
        auto [it, _] = Data.insert({str.data(), std::move(str)});
        return it->first;
    }

    // Frees a previously allocated buffer.
    void Free(const void* ptr) override
    {
        Data.erase(ptr);
    }

    size_t GetAllocatedCount() const
    {
        return Data.size();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBootstrap
{
    TStorage Storage;
    TSequenceIdGenerator SequenceIdGenerator;
    TPersistentRequestStorage RequestStorage;

    TMap<ui64, std::unique_ptr<TPendingWriteDataRequest>> PendingRequests;
    TMap<ui64, std::unique_ptr<TCachedWriteDataRequest>> CachedRequests;

    TBootstrap()
        : RequestStorage(SequenceIdGenerator, Storage)
    {}

    auto Add(ui64 nodeId, ui64 handle, ui64 offset, TString data)
        -> NThreading::TFuture<void>
    {
        auto request = std::make_shared<NProto::TWriteDataRequest>();
        request->SetNodeId(nodeId);
        request->SetHandle(handle);
        request->SetOffset(offset);
        *request->MutableBuffer() = std::move(data);

        auto res = RequestStorage.AddRequest(std::move(request));

        return std::visit(
            TOverloaded(
                [this](std::unique_ptr<TPendingWriteDataRequest> request)
                {
                    auto future = request->Promise.GetFuture();
                    PendingRequests[request->SequenceId] = std::move(request);
                    return future.IgnoreResult();
                },
                [this](std::unique_ptr<TCachedWriteDataRequest> request)
                {
                    CachedRequests[request->GetSequenceId()] =
                        std::move(request);
                    return NThreading::MakeFuture();
                }),
            std::move(res));
    }

    void SetFlushed(ui64 sequenceId)
    {
        RequestStorage.SetFlushed(CachedRequests[sequenceId].get());
    }

    void Remove(ui64 sequenceId)
    {
        RequestStorage.Remove(std::move(PendingRequests[sequenceId]));
        PendingRequests.erase(sequenceId);
    }

    void Evict(ui64 sequenceId)
    {
        RequestStorage.Evict(std::move(CachedRequests[sequenceId]));
        CachedRequests.erase(sequenceId);
    }

    bool TryProcessPendingRequests()
    {
        while (RequestStorage.HasPendingRequests()) {
            auto request = RequestStorage.TryProcessPendingRequest();
            if (!request) {
                return false;
            }

            PendingRequests[request->GetSequenceId()]->Promise.SetValue({});
            PendingRequests.erase(request->GetSequenceId());
            CachedRequests[request->GetSequenceId()] = std::move(request);
        }
        return true;
    }

    TString Dump() const
    {
        TStringBuilder out;

        out << "P[";

        for (const auto& [seqId, request]: PendingRequests) {
            out << "(" << seqId << ":" << request->Request->GetBuffer() << ")";
        }

        out << "],C[";

        for (const auto& [seqId, request]: CachedRequests) {
            out << "(" << seqId << ":" << request->GetCachedData() << ")";
        }

        out << "]";

        return out;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPersistentRequestStorageTest)
{
    Y_UNIT_TEST(Add_SetFlushed_Evict)
    {
        TBootstrap b;

        UNIT_ASSERT(!b.RequestStorage.HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            Max<ui64>(),
            b.RequestStorage.GetMinUnflushedSequenceId());

        UNIT_ASSERT(b.Add(1, 101, 1, "a").HasValue());

        UNIT_ASSERT_VALUES_EQUAL("P[],C[(1:a)]", b.Dump());
        UNIT_ASSERT_VALUES_EQUAL(1, b.Storage.GetAllocatedCount());
        UNIT_ASSERT(b.RequestStorage.HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            b.RequestStorage.GetMinUnflushedSequenceId());

        UNIT_ASSERT(b.Add(1, 102, 2, "b").HasValue());

        UNIT_ASSERT_VALUES_EQUAL("P[],C[(1:a)(2:b)]", b.Dump());
        UNIT_ASSERT_VALUES_EQUAL(2, b.Storage.GetAllocatedCount());
        UNIT_ASSERT(b.RequestStorage.HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            b.RequestStorage.GetMinUnflushedSequenceId());

        b.SetFlushed(1);
        UNIT_ASSERT_VALUES_EQUAL(2, b.Storage.GetAllocatedCount());
        UNIT_ASSERT(b.RequestStorage.HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            b.RequestStorage.GetMinUnflushedSequenceId());

        b.SetFlushed(2);
        UNIT_ASSERT_VALUES_EQUAL(2, b.Storage.GetAllocatedCount());
        UNIT_ASSERT(!b.RequestStorage.HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            Max<ui64>(),
            b.RequestStorage.GetMinUnflushedSequenceId());

        b.Evict(1);
        UNIT_ASSERT_VALUES_EQUAL(1, b.Storage.GetAllocatedCount());

        UNIT_ASSERT(b.TryProcessPendingRequests());
        UNIT_ASSERT_VALUES_EQUAL("P[],C[(2:b)]", b.Dump());

        b.Evict(2);
        UNIT_ASSERT_VALUES_EQUAL(0, b.Storage.GetAllocatedCount());

        UNIT_ASSERT(b.TryProcessPendingRequests());
        UNIT_ASSERT_VALUES_EQUAL("P[],C[]", b.Dump());
    }

    Y_UNIT_TEST(StorageFull)
    {
        TBootstrap b;
        b.Storage.Capacity = 2;

        UNIT_ASSERT(b.Add(1, 101, 1, "a").HasValue());
        UNIT_ASSERT(b.Add(1, 102, 2, "b").HasValue());

        auto add3 = b.Add(1, 102, 3, "c");
        auto add4 = b.Add(1, 102, 4, "d");
        UNIT_ASSERT(!add3.HasValue());
        UNIT_ASSERT(!add4.HasValue());

        UNIT_ASSERT_VALUES_EQUAL("P[(3:c)(4:d)],C[(1:a)(2:b)]", b.Dump());
        UNIT_ASSERT_VALUES_EQUAL(2, b.Storage.GetAllocatedCount());
        UNIT_ASSERT(b.RequestStorage.HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            b.RequestStorage.GetMinUnflushedSequenceId());

        b.SetFlushed(1);
        b.SetFlushed(2);
        UNIT_ASSERT(b.RequestStorage.HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            b.RequestStorage.GetMinUnflushedSequenceId());
        UNIT_ASSERT(!add3.HasValue());
        UNIT_ASSERT(!add4.HasValue());

        b.Evict(1);
        UNIT_ASSERT(!b.TryProcessPendingRequests());
        UNIT_ASSERT_VALUES_EQUAL("P[(4:d)],C[(2:b)(3:c)]", b.Dump());
        UNIT_ASSERT_VALUES_EQUAL(2, b.Storage.GetAllocatedCount());
        UNIT_ASSERT(b.RequestStorage.HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            b.RequestStorage.GetMinUnflushedSequenceId());
        UNIT_ASSERT(add3.HasValue());
        UNIT_ASSERT(!add4.HasValue());

        b.SetFlushed(3);
        b.Evict(2);
        UNIT_ASSERT(b.TryProcessPendingRequests());
        UNIT_ASSERT_VALUES_EQUAL("P[],C[(3:c)(4:d)]", b.Dump());
        UNIT_ASSERT_VALUES_EQUAL(2, b.Storage.GetAllocatedCount());
        UNIT_ASSERT(b.RequestStorage.HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            4,
            b.RequestStorage.GetMinUnflushedSequenceId());
        UNIT_ASSERT(add4.HasValue());

        b.SetFlushed(3);
        b.Evict(3);
        b.Evict(4);
        UNIT_ASSERT_VALUES_EQUAL("P[],C[]", b.Dump());
        UNIT_ASSERT_VALUES_EQUAL(0, b.Storage.GetAllocatedCount());
        UNIT_ASSERT(!b.RequestStorage.HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            Max<ui64>(),
            b.RequestStorage.GetMinUnflushedSequenceId());
    }

    Y_UNIT_TEST(Add_Remove)
    {
        TBootstrap b;
        b.Storage.Capacity = 1;

        UNIT_ASSERT(b.Add(1, 101, 1, "a").HasValue());

        auto add2 = b.Add(1, 102, 2, "b");
        auto add3 = b.Add(1, 102, 3, "c");
        UNIT_ASSERT(!add2.HasValue());
        UNIT_ASSERT(!add3.HasValue());

        b.Remove(2);
        b.SetFlushed(1);
        b.Evict(1);
        b.TryProcessPendingRequests();

        UNIT_ASSERT_VALUES_EQUAL("P[],C[(3:c)]", b.Dump());
        UNIT_ASSERT_VALUES_EQUAL(1, b.Storage.GetAllocatedCount());
        UNIT_ASSERT(b.RequestStorage.HasUnflushedRequests());
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            b.RequestStorage.GetMinUnflushedSequenceId());
    }
}

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
