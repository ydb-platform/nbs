#pragma once

#include <contrib/ydb/library/actors/core/event.h>
#include <contrib/ydb/library/actors/core/event_pb.h>
#include <contrib/ydb/library/actors/util/rc_buf.h>

#include <util/system/context.h>
#include <util/system/filemap.h>

#include <span>
#include <vector>

namespace NActors {

    // Coroutine-driven event serializer for the v2 data plane. Mirrors upstream YDB's
    // TCoroutineChunkSerializer as of Interconnect v2 without the RDMA memory-region tracking and
    // without the cached-sizes CodedOutputStream path, so the v1 serializer in core stays untouched.
    // Each FeedBuf resumes serialization of the current event until the provided buffer (or
    // totalSize budget) is exhausted, and returns the produced chunks: either slices of that buffer
    // or aliases into the event's own memory (ropes, strings, cords).
    class TCoroutineChunkSerializerV2 final : public TChunkSerializer, protected ITrampoLine {
    public:
        struct TChunk {
            const char* Buf;
            size_t Size;
        };

        TCoroutineChunkSerializerV2();
        ~TCoroutineChunkSerializerV2();

        // Starts serializing `event`. With `withCord`, cords are aliased into output chunks and kept
        // alive via GetCords(); otherwise their bytes are copied into the feed buffer.
        void SetSerializingEvent(const IEventBase *event, bool withCord);
        void DiscardEvent() { Event = nullptr; };
        void Abort();
        std::span<TChunk> FeedBuf(void* data, size_t size);
        std::span<TChunk> FeedBuf(TMutableContiguousSpan *buffer, size_t totalSize);
        bool IsComplete() const {
            return !Event;
        }
        bool IsSuccessfull() const {
            return SerializationSuccess;
        }
        const IEventBase *GetCurrentEvent() const {
            return Event;
        }

        bool Next(void** data, int* size) override;
        void BackUp(int count) override;
        int64_t ByteCount() const override {
            return TotalSerializedDataSize;
        }
        bool WriteAliasedRaw(const void* data, int size) override;
        bool AllowsAliasing() const override;

        bool WriteRope(const TRope *rope) override;
        bool WriteString(const TString *s) override;
        bool WriteCord(const y_absl::Cord& cord) override;

        // Cords aliased since the last FeedBuf; the caller must take them before the next FeedBuf.
        std::vector<y_absl::Cord>& GetCords() {
            return Cords;
        }

    protected:
        void DoRun() override;
        void Resume();
        void Produce(const void* data, ssize_t size);

        i64 TotalSerializedDataSize;
        TMappedAllocation Stack;
        TContClosure SelfClosure;
        TContMachineContext InnerContext;
        TContMachineContext *BufFeedContext = nullptr;
        TMutableContiguousSpan Buffer;
        size_t TotalSizeRemain;
        std::vector<TChunk> Chunks;
        TChunk LastChunk{nullptr, 0};
        const IEventBase *Event = nullptr;
        bool CancelFlag = false;
        bool AbortFlag;
        bool SerializationSuccess;
        bool Finished = false;
        bool WithCord = false;
        std::vector<y_absl::Cord> Cords;
    };

}
