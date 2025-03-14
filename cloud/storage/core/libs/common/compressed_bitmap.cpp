#include "compressed_bitmap.h"

#include <util/generic/utility.h>
#include <util/system/yassert.h>

#include <cmath>
#include <cstring>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

#define BAD_CHUNK_TYPE(t) Y_ABORT("Bad chunk type: %u", t)

////////////////////////////////////////////////////////////////////////////////

namespace NPrivate {

template <typename T>
struct TRangeInfo
{
    T First;
    T Last;
    ui32 EndBit;
};

template <typename T, ui16 chunkSize>
static TRangeInfo<T> RangeInfo(T b, T e)
{
    if (b >= e) {
        return {1, 0, 0};
    }

    TRangeInfo<T> rangeInfo;
    rangeInfo.First = b / chunkSize;
    rangeInfo.Last = (e - 1) / chunkSize;
    rangeInfo.EndBit = e % chunkSize;
    if (!rangeInfo.EndBit) {
        rangeInfo.EndBit = chunkSize;
    }
    return rangeInfo;
}

}   // namespace NPrivate

class TCompressedBitmap::TImpl
{
private:
    static const ui8 PLAIN = 0;
    static const ui8 RLE = 1;

    // just an ordinary bitmap
    struct TPlainChunkData
    {
        ui64 Data[CHUNK_SIZE / 64];
        static const ui64 MASK = 0xFFFFFFFFFFFFFFFFULL;

        TPlainChunkData()
            : Data()
        {
        }

        static ui16 PopCount(ui64 x)
        {
            // 64-bit SWAR
            // https://www.playingwithpointers.com/blog/swar.html
            ui64 byteSums = x - ((x & 0xAAAAAAAAAAAAAAAAULL) >> 1);
            byteSums = (byteSums & 0x3333333333333333ULL)
                + ((byteSums >> 2) & 0x3333333333333333ULL);
            byteSums = (byteSums + (byteSums >> 4)) & 0x0F0F0F0F0F0F0F0FULL;

            return byteSums * 0x0101010101010101ULL >> 56;
        }

        int Set(ui16 b, ui16 e)
        {
            if (b >= e) {
                return 0;
            }

            int count = 0;

            auto rangeInfo = NPrivate::RangeInfo<ui16, 64>(b, e);

            if (rangeInfo.First == rangeInfo.Last) {
                count -= PopCount(Data[rangeInfo.First]);
                Data[rangeInfo.First] |= (MASK >> (64 - e + b)) << (b % 64);
                count += PopCount(Data[rangeInfo.First]);
            } else {
                count -= PopCount(Data[rangeInfo.First]);
                Data[rangeInfo.First] |= MASK << (b % 64);
                count += PopCount(Data[rangeInfo.First]);

                count -= PopCount(Data[rangeInfo.Last]);
                Data[rangeInfo.Last] |= MASK >> (64 - rangeInfo.EndBit);
                count += PopCount(Data[rangeInfo.Last]);

                ++rangeInfo.First;
                while (rangeInfo.First < rangeInfo.Last) {
                    count += 64 - PopCount(Data[rangeInfo.First]);
                    Data[rangeInfo.First] = MASK;
                    ++rangeInfo.First;
                }
            }

            return count;
        }

        bool Bit(ui16 b) const
        {
            const auto g = Data[b / 64];
            return g & 1ULL << b % 64;
        }

        int Unset(ui16 b, ui16 e)
        {
            if (b >= e) {
                return 0;
            }

            int count = 0;

            auto rangeInfo = NPrivate::RangeInfo<ui16, 64>(b, e);

            if (rangeInfo.First == rangeInfo.Last) {
                count += PopCount(Data[rangeInfo.First]);
                Data[rangeInfo.First] &= ~((MASK >> (64 - e + b)) << (b % 64));
                count -= PopCount(Data[rangeInfo.First]);
            } else {
                count += PopCount(Data[rangeInfo.First]);
                Data[rangeInfo.First] &= ~(MASK << (b % 64));
                count -= PopCount(Data[rangeInfo.First]);

                count += PopCount(Data[rangeInfo.Last]);
                Data[rangeInfo.Last] &= ~(MASK >> (64 - rangeInfo.EndBit));
                count -= PopCount(Data[rangeInfo.Last]);

                ++rangeInfo.First;
                while (rangeInfo.First < rangeInfo.Last) {
                    count += PopCount(Data[rangeInfo.First]);
                    Data[rangeInfo.First] = 0;
                    ++rangeInfo.First;
                }
            }

            return count;
        }

        ui16 Count() const
        {
            ui16 c = 0;

            for (ui8 i = 0; i < CHUNK_SIZE / 64; ++i) {
                c += PopCount(Data[i]);
            }

            return c;
        }

        ui16 Count(ui16 b, ui16 e) const
        {
            auto rangeInfo = NPrivate::RangeInfo<ui16, 64>(b, e);

            const auto firstG = Data[rangeInfo.First];
            const auto firstEnd = rangeInfo.First == rangeInfo.Last
                ? rangeInfo.EndBit : 64;
            ui16 c = 0;
            for (ui16 i = b % 64; i < firstEnd; ++i) {
                c += !!(firstG & 1ULL << i);
            }

            if (rangeInfo.First != rangeInfo.Last) {
                const auto lastG = Data[rangeInfo.Last];
                for (ui16 i = 0; i < rangeInfo.EndBit; ++i) {
                    c += !!(lastG & 1ULL << i);
                }

                ++rangeInfo.First;
                while (rangeInfo.First < rangeInfo.Last) {
                    c += PopCount(Data[rangeInfo.First]);
                    ++rangeInfo.First;
                }
            }

            return c;
        }
    };

    struct TRun
    {
        ui16 Begin;
        ui16 End;

        explicit operator bool() const
        {
            Y_DEBUG_ABORT_UNLESS(End >= Begin);
            return End > Begin;
        }
    };

    static const ui8 MAX_RUNS = CHUNK_SIZE / 256 - 1;

    struct TCompressedChunkData
    {
        // TODO
        //  1) don't call Count() / ZeroCount() so often - in most of the cases
        //      the diff in count can be calculated without total count recalc
        //  2) cleanup all those if-s present in Set() / Unset() - I'm pretty
        //      sure that all that branching can be simplified

        // non-intersecting sorted runs
        //  [   )   [   )   [   )
        TRun Runs[MAX_RUNS];

        // returns the last run whose Begin is <= x
        // runs     [   )   [   )   [   )
        // x          ^
        // ret      [   )
        // x            ^
        // ret      [   )
        // x              ^
        // ret      [   )
        // x                ^
        // ret              [   )
        // x     ^
        // ret  0
        TRun* Find(ui16 x)
        {
            TRun* prev = nullptr;
            for (ui8 i = 0; i < MAX_RUNS; ++i) {
                auto& run = Runs[i];
                if (!run || run.Begin > x) {
                    break;
                }

                prev = &run;
            }

            return prev;
        }

        const TRun* Find(ui16 x) const
        {
            return const_cast<TCompressedChunkData*>(this)->Find(x);
        }

        void InsertBefore(TRun* run, ui16 b, ui16 e)
        {
            auto idx = ui8(run - Runs);
            for (ui8 i = MAX_RUNS - 1; i > idx; --i) {
                Runs[i] = Runs[i - 1];
            }
            Runs[idx] = {b, e};
        }

        void Delete(TRun* begin, TRun* end)
        {
            Y_DEBUG_ABORT_UNLESS(end >= begin);
            auto len = end - begin;
            if (!len) {
                return;
            }

            auto off = ui8(begin - Runs);
            ui8 i = off;
            while (i + len < MAX_RUNS) {
                Runs[i] = Runs[i + len];
                ++i;
            }
            while (i < MAX_RUNS) {
                Runs[i] = TRun();
                ++i;
            }
        }

        ui16 Count() const
        {
            ui16 c = 0;
            for (ui8 i = 0; i < MAX_RUNS; ++i) {
                const auto run = Runs[i];
                if (!run) {
                    break;
                }

                c += run.End - run.Begin;
            }
            return c;
        }

        ui16 Count(ui16 b, ui16 e) const
        {
            ui16 c = 0;
            for (ui8 i = 0; i < MAX_RUNS; ++i) {
                const auto run = Runs[i];
                if (!run) {
                    break;
                }

                if (run.End > b && run.Begin < e) {
                    c += Min(run.End, e) - Max(run.Begin, b);
                }
            }
            return c;
        }

        ui16 ZeroCount() const
        {
            return CHUNK_SIZE - Count();
        }

        int Set(ui16 b, ui16 e)
        {
            auto* pb = Find(b);
            auto* pe = Find(e);

            if (pb) {
                Y_DEBUG_ABORT_UNLESS(pe);

                if (pb->End >= b) {
                    // pb intersects with [b, e)
                    const auto c = Count();
                    pb->End = Max(e, pe->End);
                    Delete(pb + 1, pe + 1);
                    return Count() - c;
                }

                // pb doesn't intersect with [b, e)
                ++pb;
                if (pb == Runs + MAX_RUNS) {
                    return -1;
                }

                if (!*pb) {
                    *pb = {b, e};
                    return e - b;
                }

                if (pb->Begin <= e) {
                    if (e <= pb->End) {
                        const auto prevBegin = pb->Begin;
                        pb->Begin = b;
                        return prevBegin - b;
                    }

                    const auto c = Count();
                    pb->Begin = b;
                    pb->End = Max(e, pe->End);
                    Delete(pb + 1, pe + 1);
                    return Count() - c;
                }

                if (Runs[MAX_RUNS - 1]) {
                    return -1;
                }

                InsertBefore(pb, b, e);
                return e - b;
            }

            auto& firstRun = Runs[0];
            if (!firstRun) {
                // bitmap was empty
                firstRun.Begin = b;
                firstRun.End = e;
                return e - b;
            }

            if (firstRun.Begin > e) {
                if (Runs[MAX_RUNS - 1]) {
                    return -1;
                }

                InsertBefore(&firstRun, b, e);
                return e - b;
            }

            const auto prevBegin = firstRun.Begin;
            auto c = Count();
            firstRun.Begin = b;
            if (e <= firstRun.End) {
                return prevBegin - b;
            }

            Y_DEBUG_ABORT_UNLESS(pe);
            firstRun.End = Max(e, pe->End);
            Delete(&firstRun + 1, pe + 1);
            return Count() - c;
        }

        void SetAll()
        {
            Runs[0] = {0, CHUNK_SIZE};
            for (ui8 i = 1; i < MAX_RUNS; ++i) {
                Runs[i] = TRun();
            }
        }

        int Unset(ui16 b, ui16 e)
        {
            auto* pb = Find(b);
            auto* pe = Find(e);

            if (pb) {
                Y_DEBUG_ABORT_UNLESS(pe);

                if (pb->End >= b) {
                    // pb intersects with [b, e)
                    if (pb->End > e) {
                        if (Runs[MAX_RUNS - 1]) {
                            return -1;
                        }

                        if (pb->Begin == b) {
                            pb->Begin = e;
                        } else {
                            InsertBefore(pb + 1, e, pb->End);
                            pb->End = b;
                        }

                        return e - b;
                    }

                    auto c = ZeroCount();
                    pb->End = b;

                    if (pe != pb) {
                        auto off = *pb ? 1 : 0;
                        if (pe->End > e) {
                            pe->Begin = e;
                            Delete(pb + off, pe);
                        } else {
                            Delete(pb + off, pe + 1);
                        }
                    } else if (!*pb) {
                        Delete(pb, pb + 1);
                    }

                    return ZeroCount() - c;
                }

                // pb doesn't intersect with [b, e)
                if (pe != pb) {
                    auto c = ZeroCount();
                    if (pe->End > e) {
                        pe->Begin = e;
                        Delete(pb + 1, pe);
                    } else {
                        Delete(pb + 1, pe + 1);
                    }
                    return ZeroCount() - c;
                }

                return 0;
            }

            auto& firstRun = Runs[0];
            if (!firstRun || firstRun.Begin >= e) {
                return 0;
            }
            Y_DEBUG_ABORT_UNLESS(pe);

            if (firstRun.End > e) {
                const auto prevBegin = firstRun.Begin;
                firstRun.Begin = e;
                return e - prevBegin;
            }

            auto c = ZeroCount();
            if (pe->End > e) {
                pe->Begin = e;
                Delete(&firstRun, pe);
            } else {
                Delete(&firstRun, pe + 1);
            }
            return ZeroCount() - c;
        }

        bool Bit(ui16 b) const
        {
            const auto* pb = Find(b);
            return pb && pb->End > b;
        }
    };

    static_assert(sizeof(TPlainChunkData) == 128);
    static_assert(sizeof(TCompressedChunkData) == 12);
    static_assert(sizeof(TPlainChunkData*) <= sizeof(TCompressedChunkData));

    template <class T>
    struct TDataWrapper
    {
        ui8 Type;
        ui16 Count;
        T Data;
    };

    struct TChunk: TNonCopyable
    {
        union TData {
            TDataWrapper<TPlainChunkData*> Plain;
            TDataWrapper<TCompressedChunkData> Compressed;
        } Data;

        TChunk()
        {
            InitData();
            Count() = 0;
        }

        ~TChunk()
        {
            DeleteData();
        }

        ui8& Type()
        {
            return Data.Plain.Type;
        }

        ui8 Type() const
        {
            return Data.Plain.Type;
        }

        ui16& Count()
        {
            return Data.Plain.Count;
        }

        ui16 Count() const
        {
            return Data.Plain.Count;
        }

        ui16 Count(ui16 b, ui16 e) const
        {
            switch (Type()) {
                case PLAIN: {
                    return Plain()->Count(b, e);
                }

                case RLE: {
                    return Compressed()->Count(b, e);
                }
            }

            BAD_CHUNK_TYPE(Type());
        }

        void InitData()
        {
            memset(&Data.Compressed.Data, 0, sizeof(TCompressedChunkData));
            Type() = RLE;
        }

        TPlainChunkData* Plain()
        {
            Y_DEBUG_ABORT_UNLESS(Type() == PLAIN);
            return Data.Plain.Data;
        }

        const TPlainChunkData* Plain() const
        {
            Y_DEBUG_ABORT_UNLESS(Type() == PLAIN);
            return Data.Plain.Data;
        }

        TCompressedChunkData* Compressed()
        {
            Y_DEBUG_ABORT_UNLESS(Type() == RLE);
            return &Data.Compressed.Data;
        }

        const TCompressedChunkData* Compressed() const
        {
            Y_DEBUG_ABORT_UNLESS(Type() == RLE);
            return &Data.Compressed.Data;
        }

        void DeleteData()
        {
            if (Type() == PLAIN) {
                Y_DEBUG_ABORT_UNLESS(Plain());
                delete Plain();
            }

            InitData();
        }

        void CopyFrom(const TChunk& other)
        {
            DeleteData();
            Data = other.Data;

            if (other.Type() == PLAIN) {
                Data.Plain.Data = new TPlainChunkData(*other.Data.Plain.Data);
            }
        }

        void MergeFrom(const TChunk& other, ui64& compressedChunkCount)
        {
            for (ui16 i = 0; i < CHUNK_SIZE; ++i) {
                if (other.Test(i)) {
                    Set(i, i + 1, compressedChunkCount);
                }
            }
        }

        void Compress()
        {
            auto plain = Plain();
            InitData();
            ui8 runIdx = 0;
            // TODO: loop over 64-bit chunks, not single bits
            for (ui16 i = 0; i < CHUNK_SIZE; ++i) {
                if (plain->Bit(i)) {
                    if (!Compressed()->Runs[runIdx]) {
                        Compressed()->Runs[runIdx] = {i, i};
                    }
                    ++Compressed()->Runs[runIdx].End;
                } else if (Compressed()->Runs[runIdx]) {
                    ++runIdx;

                    if (runIdx == MAX_RUNS) {
                        break;
                    }
                }
            }

            delete plain;
        }

        void Decompress()
        {
            auto compressed = *Compressed();
            Type() = PLAIN;
            Data.Plain.Data = new TPlainChunkData();
            for (ui8 i = 0; i < MAX_RUNS; ++i) {
                const auto run = compressed.Runs[i];
                if (!run) {
                    break;
                }

                Plain()->Set(run.Begin, run.End);
            }
        }

        ui16 SetAll(ui64& compressedChunkCount)
        {
            compressedChunkCount += Type() == PLAIN;
            const auto c = Count();
            DeleteData();
            Compressed()->SetAll();
            Count() = CHUNK_SIZE;
            return Count() - c;
        }

        ui16 UnsetAll(ui64& compressedChunkCount)
        {
            compressedChunkCount += Type() == PLAIN;
            const auto c = Count();
            DeleteData();
            Count() = 0;
            return c;
        }

#define VERIFY_RANGE(b, e)                      \
        Y_DEBUG_ABORT_UNLESS(b < CHUNK_SIZE);         \
        Y_DEBUG_ABORT_UNLESS(e <= CHUNK_SIZE);        \
        Y_DEBUG_ABORT_UNLESS(b < e);                  \
// VERIFY_RANGE

        ui16 Set(ui32 b, ui32 e, ui64& compressedChunkCount)
        {
            VERIFY_RANGE(b, e);

            if (Count() == CHUNK_SIZE) {
                return 0;
            }

            switch (Type()) {
                case PLAIN: {
                    const auto c = Plain()->Set(b, e);
                    Count() += c;
                    if (Count() > CHUNK_SIZE - MAX_RUNS) {
                        Compress();
                        ++compressedChunkCount;
                    }
                    return c;
                }

                case RLE: {
                    auto c = Compressed()->Set(b, e);
                    if (c < 0) {
                        Decompress();
                        --compressedChunkCount;
                        c = Plain()->Set(b, e);
                    }

                    Count() += c;
                    return c;
                }
            }

            BAD_CHUNK_TYPE(Type());
        }

        ui16 Unset(ui32 b, ui32 e, ui64& compressedChunkCount)
        {
            VERIFY_RANGE(b, e);

            if (Count() == 0) {
                return 0;
            }

            switch (Type()) {
                case PLAIN: {
                    const auto c = Plain()->Unset(b, e);
                    Count() -= c;
                    if (Count() == 0) {
                        DeleteData();
                        ++compressedChunkCount;
                    } else if (Count() <= MAX_RUNS) {
                        Compress();
                        ++compressedChunkCount;
                    }
                    return c;
                }

                case RLE: {
                    auto c = Compressed()->Unset(b, e);
                    if (c < 0) {
                        Decompress();
                        --compressedChunkCount;
                        c = Plain()->Unset(b, e);
                    }

                    Count() -= c;
                    return c;
                }
            }

            BAD_CHUNK_TYPE(Type());
        }

        bool Test(ui16 b) const
        {
            VERIFY_RANGE(b, b + 1);

            if (Count() == 0) {
                return false;
            }

            switch (Type()) {
                case PLAIN: {
                    return Plain()->Bit(b);
                }

                case RLE: {
                    return Compressed()->Bit(b);
                }
            }

            BAD_CHUNK_TYPE(Type());
        }

#undef VERIFY_RANGE
    };

    static_assert(sizeof(TChunk) == 16);

public:
    static ui32 Serialize(const TChunk& chunk, char* buffer)
    {
        // TODO: endianness
        buffer[0] = chunk.Type();
        switch (chunk.Type()) {
            case PLAIN: {
                auto& plain = chunk.Data.Plain;
                memcpy(
                    buffer + 1,
                    plain.Data->Data,
                    sizeof(TPlainChunkData)
                );

                return 1 + sizeof(TPlainChunkData);
            }

            case RLE: {
                auto& compressed = chunk.Data.Compressed;
                memcpy(
                    buffer + 1,
                    compressed.Data.Runs,
                    sizeof(TCompressedChunkData)
                );

                return 1 + sizeof(TCompressedChunkData);
            }
        }

        BAD_CHUNK_TYPE(chunk.Type());
    }

    static void Parse(const TStringBuf data, TChunk* chunk, ui64& compressedChunkCount)
    {
        // TODO: endianness
        chunk->UnsetAll(compressedChunkCount);
        Y_DEBUG_ABORT_UNLESS(data.size() > 0);
        switch (chunk->Type() = data[0]) {
            case PLAIN: {
                Y_DEBUG_ABORT_UNLESS(data.size() >= sizeof(TPlainChunkData) + 1);
                auto& plain = chunk->Data.Plain;
                plain.Data = new TPlainChunkData;
                memcpy(
                    plain.Data->Data,
                    data.data() + 1,
                    sizeof(TPlainChunkData)
                );
                chunk->Count() = plain.Data->Count();
                --compressedChunkCount;

                return;
            }

            case RLE: {
                Y_DEBUG_ABORT_UNLESS(data.size() >= sizeof(TCompressedChunkData) + 1);
                auto& compressed = chunk->Data.Compressed;
                memcpy(
                    compressed.Data.Runs,
                    data.data() + 1,
                    sizeof(TCompressedChunkData)
                );
                chunk->Count() = compressed.Data.Count();

                return;
            }
        }

        BAD_CHUNK_TYPE(chunk->Type());
    }

    static ui32 ByteSize(const TChunk& chunk)
    {
        switch (chunk.Type()) {
            case PLAIN: {
                return 1 + sizeof(TPlainChunkData);
            }

            case RLE: {
                return 1 + sizeof(TCompressedChunkData);
            }
        }

        BAD_CHUNK_TYPE(chunk.Type());
    }

    static ui64 ComputeChunkCount(size_t size)
    {
        return ceil(double(size) / CHUNK_SIZE);
    }

    static ui64 ComputeCapacity(size_t size)
    {
        return ComputeChunkCount(size) * CHUNK_SIZE;
    }

    using TChunkRangeInfo = NPrivate::TRangeInfo<ui64>;

public:
    TImpl(size_t size)
        : ChunkCount(ComputeChunkCount(size))
        , CompressedChunkCount(ChunkCount)
        , Chunks(std::make_unique<TChunk[]>(ChunkCount))
    {
    }

    ~TImpl() = default;

public:
    [[nodiscard]] TRangeSerializer RangeSerializer(ui64 b, ui64 e) const
    {
        const auto rangeInfo = NPrivate::RangeInfo<ui64, CHUNK_SIZE>(b, e);
        return {*this, rangeInfo.First, rangeInfo.Last};
    }

    static bool IsZeroChunk(const TSerializedChunk& chunk)
    {
        TChunk target;
        ui64 dummyVal = 0;
        Parse(chunk.Data, &target, dummyVal);
        return target.Count() == 0;
    }

    ui64 Set(ui64 b, ui64 e)
    {
        if (b >= e) {
            return 0;
        }

        ui64 c = 0;
        const auto rangeInfo = NPrivate::RangeInfo<ui64, CHUNK_SIZE>(b, e);
        Y_DEBUG_ABORT_UNLESS(rangeInfo.Last < ChunkCount);

        c += Chunks[rangeInfo.First].Set(
            b % CHUNK_SIZE,
            rangeInfo.First == rangeInfo.Last ? rangeInfo.EndBit : CHUNK_SIZE,
            CompressedChunkCount
        );

        if (rangeInfo.First != rangeInfo.Last) {
            c += Chunks[rangeInfo.Last].Set(
                0,
                rangeInfo.EndBit,
                CompressedChunkCount
            );

            for (ui64 k = rangeInfo.First + 1; k < rangeInfo.Last; ++k) {
                c += Chunks[k].SetAll(CompressedChunkCount);
            }
        }

        return c;
    }

    ui64 Unset(ui64 b, ui64 e)
    {
        if (b >= e) {
            return 0;
        }

        ui64 c = 0;
        const auto rangeInfo = NPrivate::RangeInfo<ui64, CHUNK_SIZE>(b, e);
        Y_DEBUG_ABORT_UNLESS(rangeInfo.Last < ChunkCount);

        c += Chunks[rangeInfo.First].Unset(
            b % CHUNK_SIZE,
            rangeInfo.First == rangeInfo.Last ? rangeInfo.EndBit : CHUNK_SIZE,
            CompressedChunkCount
        );

        if (rangeInfo.First != rangeInfo.Last) {
            c += Chunks[rangeInfo.Last].Unset(
                0,
                rangeInfo.EndBit,
                CompressedChunkCount
            );

            for (ui64 k = rangeInfo.First + 1; k < rangeInfo.Last; ++k) {
                c += Chunks[k].UnsetAll(CompressedChunkCount);
            }
        }

        return c;
    }

    void Update(const TSerializedChunk& chunk)
    {
        Y_DEBUG_ABORT_UNLESS(chunk.ChunkIdx < ChunkCount);
        auto& target = Chunks[chunk.ChunkIdx];
        Parse(chunk.Data, &target, CompressedChunkCount);
    }

    ui64 Update(const TCompressedBitmap::TImpl& other, ui64 b)
    {
        const auto j = b / CHUNK_SIZE;
        Y_DEBUG_ABORT_UNLESS(b % CHUNK_SIZE == 0);
        ui64 c = 0;
        for (ui64 i = 0; i < other.ChunkCount && i + j < ChunkCount; ++i) {
            const auto& source = other.Chunks[i];
            auto& target = Chunks[j + i];
            c += source.Count();
            target.CopyFrom(source);
        }
        return c;
    }

    ui64 Merge(const TSerializedChunk& chunk)
    {
        if (chunk.ChunkIdx >= ChunkCount) {
            return 0;
        }

        auto& target = Chunks[chunk.ChunkIdx];
        if (target.Count() == 0) {
            Parse(chunk.Data, &target, CompressedChunkCount);
        } else {
            TChunk targetCopy;
            targetCopy.CopyFrom(target);
            Parse(chunk.Data, &target, CompressedChunkCount);
            target.MergeFrom(targetCopy, CompressedChunkCount);
        }
        return target.Count();
    }

    [[nodiscard]] ui64 Count() const
    {
        return Count(0, ChunkCount * CHUNK_SIZE);
    }

    [[nodiscard]] ui64 Count(ui64 b, ui64 e) const
    {
        const auto rangeInfo = NPrivate::RangeInfo<ui64, CHUNK_SIZE>(b, e);
        Y_DEBUG_ABORT_UNLESS(rangeInfo.Last < ChunkCount);

        ui64 c = Chunks[rangeInfo.First].Count(
            b % CHUNK_SIZE,
            rangeInfo.First == rangeInfo.Last ? rangeInfo.EndBit : CHUNK_SIZE
        );

        if (rangeInfo.First != rangeInfo.Last) {
            c += Chunks[rangeInfo.Last].Count(
                0,
                rangeInfo.EndBit
            );

            for (ui64 k = rangeInfo.First + 1; k < rangeInfo.Last; ++k) {
                c += Chunks[k].Count();
            }
        }

        return c;
    }

    [[nodiscard]] bool Test(ui64 i) const
    {
        Y_DEBUG_ABORT_UNLESS(i / CHUNK_SIZE < ChunkCount);
        return Chunks[i / CHUNK_SIZE].Test(i % CHUNK_SIZE);
    }

    [[nodiscard]] ui64 MemSize() const {
        Y_DEBUG_ABORT_UNLESS(ChunkCount >= CompressedChunkCount);
        const auto plainChunkCount = ChunkCount - CompressedChunkCount;
        return ChunkCount * sizeof(TChunk)
            + plainChunkCount * sizeof(TPlainChunkData);
    }

private:
    const ui64 ChunkCount;
    ui64 CompressedChunkCount;
    const std::unique_ptr<TChunk[]> Chunks;

    friend class TCompressedBitmap::TRangeSerializer;
};

TCompressedBitmap::TRangeSerializer::TRangeSerializer(
        const TCompressedBitmap::TImpl& parent,
        ui64 first,
        ui64 last)
    : Parent(&parent)
    , First(first)
    , Last(last)
    , BufferPos(0)
{
    ui64 size = 0;
    for (ui64 i = First; i <= Last; ++i) {
        size += TCompressedBitmap::TImpl::ByteSize(Parent->Chunks[i]);
    }
    Buffer = size ? std::make_unique<char[]>(size) : nullptr;
}

TCompressedBitmap::TRangeSerializer::~TRangeSerializer() = default;

bool TCompressedBitmap::TRangeSerializer::Next(TSerializedChunk* sc)
{
    if (!Parent) {
        return false;
    }

    if (First > Last) {
        return false;
    }

    Y_DEBUG_ABORT_UNLESS(First < Parent->ChunkCount);
    const auto& chunk = Parent->Chunks[First];
    sc->ChunkIdx = First;
    auto len = TCompressedBitmap::TImpl::Serialize(chunk, Buffer.get() + BufferPos);
    sc->Data = TStringBuf(Buffer.get() + BufferPos, len);
    BufferPos += len;
    ++First;
    return true;
}

TCompressedBitmap::TCompressedBitmap(size_t size)
    : Size(size)
{
    Y_ABORT_UNLESS(Size);
}

TCompressedBitmap::~TCompressedBitmap() = default;

TCompressedBitmap::TCompressedBitmap(
    TCompressedBitmap&& other) noexcept = default;

TCompressedBitmap& TCompressedBitmap::operator=(
    TCompressedBitmap&& other) noexcept = default;

void TCompressedBitmap::Clear()
{
    Impl.Reset();
}

ui64 TCompressedBitmap::Set(ui64 b, ui64 e)
{
    Init();
    return Impl->Set(b, e);
}

ui64 TCompressedBitmap::Unset(ui64 b, ui64 e)
{
    Init();
    return Impl->Unset(b, e);
}

void TCompressedBitmap::Update(const TSerializedChunk& chunk)
{
    Init();
    Impl->Update(chunk);
}

ui64 TCompressedBitmap::Update(const TCompressedBitmap& other, ui64 b)
{
    if (!other.Impl) {
        return 0;
    }

    Init();
    return Impl->Update(*other.Impl, b);
}

ui64 TCompressedBitmap::Merge(const TSerializedChunk& chunk)
{
    Init();
    return Impl->Merge(chunk);
}

ui64 TCompressedBitmap::Count() const
{
    if (!Impl) {
        return 0;
    }

    return Impl->Count();
}

ui64 TCompressedBitmap::Count(ui64 b, ui64 e) const
{
    if (!Impl) {
        return 0;
    }

    return Impl->Count(b, e);
}

bool TCompressedBitmap::Test(ui64 i) const
{
    if (!Impl) {
        return false;
    }

    return Impl->Test(i);
}

ui64 TCompressedBitmap::Capacity() const
{
    return TImpl::ComputeCapacity(Size);
}

ui64 TCompressedBitmap::MemSize() const
{
    if (!Impl) {
        return 0;
    }

    return Impl->MemSize();
}

TCompressedBitmap::TRangeSerializer TCompressedBitmap::RangeSerializer(
    ui64 b, ui64 e) const
{
    if (!Impl) {
        return TCompressedBitmap::TRangeSerializer();
    }

    return Impl->RangeSerializer(b, e);
}

bool TCompressedBitmap::IsZeroChunk(const TSerializedChunk& chunk)
{
    return TImpl::IsZeroChunk(chunk);
}

std::pair<ui32, ui32> TCompressedBitmap::ChunkRange(ui64 b, ui64 e)
{
    const auto rangeInfo = NPrivate::RangeInfo<ui64, CHUNK_SIZE>(b, e);
    return {rangeInfo.First, rangeInfo.Last};
}

void TCompressedBitmap::Init()
{
    if (!Impl) {
        Impl.Reset(new TImpl(Size));
    }
}

}   // namespace NCloud
