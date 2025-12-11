#include "table.h"

#include <library/cpp/aio/aio.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/hash.h>
#include <util/generic/size_literals.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/system/file.h>
#include <util/system/info.h>
#include <util/system/spinlock.h>
#include <util/thread/lfstack.h>

#include <cstring>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

const ui32 BlockSize = 4_KB;

////////////////////////////////////////////////////////////////////////////////

struct TIndices
{
    ui64 BlockIndex;
    ui64 Offset;
    ui64 PhysicalBlockIndex;
    ui64 PhysicalOffset;
    ui64 OffsetInBlock;

    TIndices(ui64 blockIndex)
        : BlockIndex(blockIndex)
        , Offset(BlockIndex * sizeof(TBlockMeta))
        , PhysicalBlockIndex(Offset / BlockSize)
        , PhysicalOffset(PhysicalBlockIndex * BlockSize)
        , OffsetInBlock(Offset - PhysicalOffset)
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TMetaTable::TImpl
{
    mutable TFileHandle File;
    mutable NAsyncIO::TAsyncIOService AsyncIO;

    struct TWriteRequestContext
    {
        ui64 BlockIndex = 0;
        TBlockMeta Meta;
        NThreading::TPromise<void> Promise;
    };

    struct TPhysicalBlockContext
    {
        bool Busy = false;
        TDeque<TWriteRequestContext> WriteQueue;
    };
    THashMap<ui64, TPhysicalBlockContext> Block2Context;
    TAdaptiveLock WriteLock;

    TAdaptiveLock DebugLock;

public:
    TImpl(const TString& filePath, ui64 blockCount)
        : File(filePath, EOpenModeFlag::DirectAligned | EOpenModeFlag::RdWr)
    {
        Y_ABORT_UNLESS(File.IsOpen());
        Y_UNUSED(blockCount);
        // File.Resize(blockCount * sizeof(TBlockMeta));
        AsyncIO.Start();
    }

    ~TImpl()
    {
        AsyncIO.Stop();
        File.Close();
    }

    NThreading::TFuture<TBlockMeta> Read(ui64 blockIndex) const
    {
        TIndices i(blockIndex);

        Cdbg << "async read: " << i.BlockIndex << "(" << i.PhysicalBlockIndex
             << ")" << Endl;

        TString buffer(2 * BlockSize, 0);
        const ui64 baseAddr = reinterpret_cast<ui64>(buffer.data());
        const ui64 alignedAddr = AlignUp<ui64>(baseAddr, BlockSize);
        const auto alignmentOffset = alignedAddr - baseAddr;
        auto alignedPtr = const_cast<char*>(buffer.data() + alignmentOffset);

        return AsyncIO.Read(File, alignedPtr, BlockSize, i.PhysicalOffset)
            .Apply(
                [b = std::move(buffer), alignedPtr, i](const auto& f)
                {
                    try {
                        Cdbg << "async read completed: " << i.BlockIndex << "("
                             << i.PhysicalBlockIndex << ")" << Endl;

                        f.GetValue();

                        return *reinterpret_cast<const TBlockMeta*>(
                            alignedPtr + i.OffsetInBlock);
                    } catch (...) {
                        Cerr << CurrentExceptionMessage() << Endl;
                        Y_ABORT_UNLESS(0);
                    }
                });
    }

    NThreading::TFuture<void> WriteImpl(TBlockMeta meta, TIndices i)
    {
        TString data(reinterpret_cast<const char*>(&meta), sizeof(meta));

        Cdbg << "async write: " << i.BlockIndex << "(" << i.PhysicalBlockIndex
             << ")" << Endl;

        TString buffer(2 * BlockSize, 0);
        const ui64 baseAddr = reinterpret_cast<ui64>(buffer.data());
        const ui64 alignedAddr = AlignUp<ui64>(baseAddr, BlockSize);
        const auto alignmentOffset = alignedAddr - baseAddr;
        auto alignedPtr = const_cast<char*>(buffer.data() + alignmentOffset);

        // rmw
        return AsyncIO.Read(File, alignedPtr, BlockSize, i.PhysicalOffset)
            .Apply(
                [this,
                 b = std::move(buffer),
                 d = std::move(data),
                 i,
                 alignedPtr](const auto& f) mutable
                {
                    try {
                        f.GetValue();

                        char* p = alignedPtr + i.OffsetInBlock;
                        std::memcpy(p, d.data(), d.size());

                        return AsyncIO
                            .Write(
                                File,
                                alignedPtr,
                                BlockSize,
                                i.PhysicalOffset)
                            .Apply(
                                [this, b2 = std::move(b), i](const auto& f)
                                {
                                    try {
                                        Cdbg << "async write completed: "
                                             << i.BlockIndex << "("
                                             << i.PhysicalBlockIndex << ")"
                                             << Endl;

                                        f.GetValue();

                                        with_lock (WriteLock) {
                                            // Cdbg << "looking for write
                                            // context for physical block "
                                            //     << i.PhysicalBlockIndex <<
                                            //     Endl;

                                            auto it = Block2Context.find(
                                                i.PhysicalBlockIndex);
                                            Y_ABORT_UNLESS(
                                                it != Block2Context.end());
                                            auto& context = it->second;
                                            if (context.WriteQueue) {
                                                auto request =
                                                    context.WriteQueue.front();
                                                context.WriteQueue.pop_front();
                                                WriteImpl(
                                                    request.Meta,
                                                    TIndices(
                                                        request.BlockIndex))
                                                    .Subscribe(
                                                        [p = std::move(
                                                             request.Promise)](
                                                            const auto&) mutable
                                                        { p.SetValue(); });
                                            } else {
                                                Block2Context.erase(it);
                                                // Cdbg << "destroyed write
                                                // context for physical block "
                                                //     << i.PhysicalBlockIndex
                                                //     << Endl;
                                            }
                                        }
                                    } catch (...) {
                                        Cerr << CurrentExceptionMessage()
                                             << Endl;
                                        Y_ABORT_UNLESS(0);
                                    }
                                });
                    } catch (...) {
                        Cerr << CurrentExceptionMessage() << Endl;
                        Y_ABORT_UNLESS(0);
                    }
                });
    }

    NThreading::TFuture<void> Write(ui64 blockIndex, TBlockMeta meta)
    {
        TIndices i(blockIndex);

        with_lock (WriteLock) {
            auto& context = Block2Context[i.PhysicalBlockIndex];

            if (context.Busy) {
                context.WriteQueue.push_back({
                    blockIndex,
                    meta,
                    NThreading::NewPromise<void>(),
                });

                Cdbg << "async write postponed: " << blockIndex << "("
                     << i.PhysicalBlockIndex << ")" << Endl;

                return context.WriteQueue.back().Promise;
            }

            // Cdbg << "created write context for physical block "
            //     << i.PhysicalBlockIndex << Endl;

            context.Busy = true;
        }

        return WriteImpl(meta, i);
    }
};

////////////////////////////////////////////////////////////////////////////////

TMetaTable::TMetaTable(const TString& filePath, ui64 blockCount)
    : Impl(new TImpl(filePath, blockCount))
{}

TMetaTable::~TMetaTable()
{}

NThreading::TFuture<TBlockMeta> TMetaTable::Read(ui64 blockIndex) const
{
    return Impl->Read(blockIndex);
}

NThreading::TFuture<void> TMetaTable::Write(ui64 blockIndex, TBlockMeta meta)
{
    return Impl->Write(blockIndex, meta);
}

}   // namespace NCloud::NBlockStore
