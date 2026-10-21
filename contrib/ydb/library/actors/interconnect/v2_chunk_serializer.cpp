#include "v2_chunk_serializer.h"

#include <contrib/ydb/library/actors/util/rope.h>

namespace NActors {

    TCoroutineChunkSerializerV2::TCoroutineChunkSerializerV2()
        : TotalSerializedDataSize(0)
        , Stack(64 * 1024)
        , SelfClosure{this, TArrayRef(Stack.Begin(), Stack.End())}
        , InnerContext(SelfClosure)
    {}

    TCoroutineChunkSerializerV2::~TCoroutineChunkSerializerV2() {
        CancelFlag = true;
        Resume();
        Y_ABORT_UNLESS(Finished);
    }

    bool TCoroutineChunkSerializerV2::AllowsAliasing() const {
        return true;
    }

    void TCoroutineChunkSerializerV2::Produce(const void* data, ssize_t size) {
        Y_ABORT_UNLESS(size < 0 || static_cast<size_t>(size) <= TotalSizeRemain);
        TotalSizeRemain -= size;
        TotalSerializedDataSize += size;

        if (LastChunk.Buf + LastChunk.Size == data) {
            Y_ABORT_UNLESS(size > 0 || static_cast<size_t>(-size) <= LastChunk.Size);
            LastChunk.Size += size;
            if (LastChunk.Size == 0) {
                Chunks.pop_back();
                LastChunk = Chunks.empty() ? TChunk{nullptr, 0} : Chunks.back();
            } else {
                Chunks.back().Size += size;
            }
            return;
        }

        Y_ABORT_UNLESS(size > 0);

        LastChunk = Chunks.emplace_back(TChunk{
            .Buf = static_cast<const char*>(data),
            .Size = static_cast<size_t>(size),
        });
    }

    bool TCoroutineChunkSerializerV2::WriteAliasedRaw(const void* data, int size) {
        Y_ABORT_UNLESS(!CancelFlag);
        Y_ABORT_UNLESS(!AbortFlag);
        Y_ABORT_UNLESS(size >= 0);

        while (size) {
            if (const size_t bytesToAppend = Min<size_t>(size, TotalSizeRemain)) {
                const void *produce = data;
                // if the data is on the same cache line, we better copy it to the buffer
                if ((reinterpret_cast<uintptr_t>(data) & 63) + bytesToAppend <= 64 && Buffer.size() >= bytesToAppend) {
                    memcpy(Buffer.data(), data, bytesToAppend);
                    produce = Buffer.data();
                    Buffer = {Buffer.data() + bytesToAppend, Buffer.size() - bytesToAppend};
                }
                Produce(produce, bytesToAppend);
                data = static_cast<const char*>(data) + bytesToAppend;
                size -= bytesToAppend;
            } else {
                InnerContext.SwitchTo(BufFeedContext);
                if (CancelFlag || AbortFlag) {
                    return false;
                }
            }
        }

        return true;
    }

    bool TCoroutineChunkSerializerV2::Next(void** data, int* size) {
        Y_ABORT_UNLESS(!CancelFlag);
        Y_ABORT_UNLESS(!AbortFlag);

        // number of bytes we can allocate right now
        size_t maxBytes = Min(Buffer.size(), TotalSizeRemain);

        if (!maxBytes) {
            InnerContext.SwitchTo(BufFeedContext);
            if (CancelFlag || AbortFlag) {
                return false;
            }

            // recalculate actual value as it has changed
            maxBytes = Min(Buffer.size(), TotalSizeRemain);
        }

        Y_ABORT_UNLESS(maxBytes);
        *data = Buffer.data();
        *size = maxBytes;
        Buffer = Buffer.SubSpan(maxBytes, Max<size_t>());
        Produce(*data, *size);
        return true;
    }

    void TCoroutineChunkSerializerV2::BackUp(int count) {
        if (!count) {
            return;
        }
        Produce(Buffer.data(), -count);
        Buffer = {
            Buffer.data() - count,
            Buffer.size() + count,
        };
    }

    void TCoroutineChunkSerializerV2::Resume() {
        TContMachineContext feedContext;
        BufFeedContext = &feedContext;
        feedContext.SwitchTo(&InnerContext);
        BufFeedContext = nullptr;
    }

    bool TCoroutineChunkSerializerV2::WriteRope(const TRope *rope) {
        for (auto iter = rope->Begin(); iter.Valid(); iter.AdvanceToNextContiguousBlock()) {
            if (!WriteAliasedRaw(iter.ContiguousData(), iter.ContiguousSize())) {
                return false;
            }
        }
        return true;
    }

    bool TCoroutineChunkSerializerV2::WriteString(const TString *s) {
        return WriteAliasedRaw(s->data(), s->length());
    }

    bool TCoroutineChunkSerializerV2::WriteCord(const y_absl::Cord& cord) {
        if (WithCord) {
            for (const y_absl::string_view& chunk : cord.Chunks()) {
                if (!WriteAliasedRaw(chunk.data(), chunk.size())) {
                    return false;
                }
            }
            // retain ownership of the cord
            Cords.push_back(cord);
        } else {
            for (const y_absl::string_view& chunk : cord.Chunks()) {
                const char *chunkData = chunk.data();
                size_t chunkSize = chunk.size();
                while (chunkSize) {
                    void *buffer;
                    int bufferSize;
                    if (!Next(&buffer, &bufferSize)) {
                        return false;
                    }
                    size_t numBytesToCopy = Min<size_t>(chunkSize, bufferSize);
                    memcpy(buffer, chunkData, numBytesToCopy);
                    chunkData += numBytesToCopy;
                    chunkSize -= numBytesToCopy;
                    buffer = (char*)buffer + numBytesToCopy;
                    bufferSize -= numBytesToCopy;
                    BackUp(bufferSize);
                }
            }
        }
        return true;
    }

    std::span<TCoroutineChunkSerializerV2::TChunk> TCoroutineChunkSerializerV2::FeedBuf(void* data, size_t size) {
        TMutableContiguousSpan buffer(static_cast<char*>(data), size);
        return FeedBuf(&buffer, size);
    }

    std::span<TCoroutineChunkSerializerV2::TChunk> TCoroutineChunkSerializerV2::FeedBuf(TMutableContiguousSpan *buffer,
            size_t totalSize) {
        // fill in base params
        Buffer = *buffer;
        TotalSizeRemain = totalSize;
        Y_DEBUG_ABORT_UNLESS(TotalSizeRemain);

        // transfer control to the coroutine
        Y_ABORT_UNLESS(Event);
        Chunks.clear();
        LastChunk = {nullptr, 0};
        Y_ABORT_UNLESS(Cords.empty());
        Resume();

        Y_DEBUG_ABORT_UNLESS(Buffer.data() >= buffer->data() &&
            Buffer.data() + Buffer.size() <= buffer->data() + buffer->size());
        *buffer = Buffer;
        return Chunks;
    }

    void TCoroutineChunkSerializerV2::SetSerializingEvent(const IEventBase *event, bool withCord) {
        Y_ABORT_UNLESS(Event == nullptr);
        Event = event;
        TotalSerializedDataSize = 0;
        AbortFlag = false;
        WithCord = withCord;
    }

    void TCoroutineChunkSerializerV2::Abort() {
        Y_ABORT_UNLESS(Event);
        AbortFlag = true;
        Resume();
    }

    void TCoroutineChunkSerializerV2::DoRun() {
        while (!CancelFlag) {
            Y_ABORT_UNLESS(Event);
            SerializationSuccess = !AbortFlag && Event->SerializeToArcadiaStream(this);
            Event = nullptr;
            if (!CancelFlag) { // cancel flag may have been received during serialization
                InnerContext.SwitchTo(BufFeedContext);
            }
        }
        Finished = true;
        InnerContext.SwitchTo(BufFeedContext);
    }

}
