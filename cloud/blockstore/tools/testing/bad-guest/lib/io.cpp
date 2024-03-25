#include "io.h"

#include <library/cpp/aio/aio.h>

#include <cstring>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TIO::TImpl
{
    mutable TFileHandle File;
    mutable NAsyncIO::TAsyncIOService AsyncIO;
    const ui32 BlockSize;

public:
    TImpl(const TString& filePath, ui32 blockSize)
        : File(
            filePath,
            EOpenModeFlag::DirectAligned | EOpenModeFlag::RdWr
        )
        , BlockSize(blockSize)
    {
        Y_ABORT_UNLESS(File.IsOpen());
        AsyncIO.Start();
    }

    ~TImpl()
    {
        AsyncIO.Stop();
        File.Close();
    }

    TString Read(ui64 blockIndex, ui32 blockCount) const
    {
        const auto sz = blockCount * BlockSize;
        TString buffer(sz + BlockSize, 0);
        const ui64 baseAddr = reinterpret_cast<ui64>(buffer.data());
        const ui64 alignedAddr = AlignUp<ui64>(baseAddr, BlockSize);
        const auto alignmentOffset = alignedAddr - baseAddr;
        char* alignedPtr = buffer.begin() + alignmentOffset;
        const auto offset = blockIndex * BlockSize;

        auto f = AsyncIO.Read(File, alignedPtr, sz, offset);
        auto res = f.GetValueSync();
        Y_ABORT_UNLESS(res == sz);
        return {alignedPtr, sz};
    }

    static void DumbMemcpy(char* dst, const char* src, ui64 sz)
    {
        // we need this loop to be as simple as possible - no simd
        for (ui64 i = 0; i < sz; ++i) {
            dst[i] = src[i];
        }
    }

    void AlternatingWrite(ui64 blockIndex, const TVector<TStringBuf>& datas)
    {
        ui64 sz = 0;
        for (const auto& data: datas) {
            sz = Max(sz, data.Size());
        }
        sz = AlignUp<ui64>(sz);
        TString buffer(sz + BlockSize, 0);
        const ui64 baseAddr = reinterpret_cast<ui64>(buffer.data());
        const ui64 alignedAddr = AlignUp<ui64>(baseAddr, BlockSize);
        const auto alignmentOffset = alignedAddr - baseAddr;
        auto* alignedPtr = buffer.begin() + alignmentOffset;
        const auto offset = blockIndex * BlockSize;
        memset(alignedPtr, 0, sz);
        DumbMemcpy(alignedPtr, datas[0].Data(), datas[0].Size());

        auto f = AsyncIO.Write(File, alignedPtr, sz, offset);
        ui32 i = 0;
        while (!f.HasValue()) {
            const auto& data = datas[++i % datas.size()];
            DumbMemcpy(alignedPtr, data.Data(), data.Size());
        }

        Y_ABORT_UNLESS(f.GetValue() == sz);
    }
};

////////////////////////////////////////////////////////////////////////////////

TIO::TIO(const TString& filePath, ui32 blockSize)
    : Impl(new TImpl(filePath, blockSize))
{
}

TIO::~TIO() = default;

[[nodiscard]] TString TIO::Read(ui64 blockIndex, ui32 blockCount) const
{
    return Impl->Read(blockIndex, blockCount);
}

void TIO::AlternatingWrite(ui64 blockIndex, const TVector<TStringBuf>& datas)
{
    return Impl->AlternatingWrite(blockIndex, datas);
}

}   // namespace NCloud::NBlockStore
