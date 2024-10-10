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
        : File(filePath, EOpenModeFlag::DirectAligned | EOpenModeFlag::RdWr)
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

    struct TAlignedBuffer
    {
        TString Buffer;
        char* AlignedPtr;

        TAlignedBuffer(ui64 sz, ui32 align)
            : Buffer(sz + align, 0)
        {
            const ui64 baseAddr = reinterpret_cast<ui64>(Buffer.data());
            const ui64 alignedAddr = AlignUp<ui64>(baseAddr, align);
            const auto alignmentOffset = alignedAddr - baseAddr;
            AlignedPtr = Buffer.begin() + alignmentOffset;
        }
    };

    TString Read(ui64 blockIndex, ui32 blockCount) const
    {
        const auto sz = blockCount * BlockSize;
        TAlignedBuffer buffer(sz, BlockSize);
        const auto offset = blockIndex * BlockSize;

        auto f = AsyncIO.Read(File, buffer.AlignedPtr, sz, offset);
        auto res = f.GetValueSync();
        Y_ABORT_UNLESS(res == sz);
        return {buffer.AlignedPtr, sz};
    }

    void AlternatingWrite(ui64 blockIndex, const TVector<TStringBuf>& datas)
    {
        ui64 sz = 0;
        for (const auto& data: datas) {
            sz = Max(sz, data.size());
        }
        sz = AlignUp<ui64>(sz);
        TAlignedBuffer buffer(sz, BlockSize);
        const auto offset = blockIndex * BlockSize;
        memcpy(buffer.AlignedPtr, datas[0].data(), datas[0].size());

        auto f = AsyncIO.Write(File, buffer.AlignedPtr, sz, offset);
        // corrupting the data buffer that we have just sent to the storage
        // layer via AsyncIO.Write - we want to test how nbs would handle such
        // a situation - nbs should not break in any way
        ui32 i = 0;
        while (!f.HasValue()) {
            const auto& data = datas[++i % datas.size()];
            memcpy(buffer.AlignedPtr, data.data(), data.size());
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

TString TIO::Read(ui64 blockIndex, ui32 blockCount) const
{
    return Impl->Read(blockIndex, blockCount);
}

void TIO::AlternatingWrite(ui64 blockIndex, const TVector<TStringBuf>& datas)
{
    return Impl->AlternatingWrite(blockIndex, datas);
}

}   // namespace NCloud::NBlockStore
