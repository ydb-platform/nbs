#include "block_handler.h"

#include <cloud/storage/core/libs/common/sglist_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/bitmap.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString GetBlockContent(char fill = 0, size_t size = DefaultBlockSize)
{
    return TString(size, fill);
}

////////////////////////////////////////////////////////////////////////////////

IWriteBlocksHandlerPtr CreateTestWriteBlocksHandler(
    TBlockRange64 range,
    const TString& content)
{
    TSgList sglist(range.Size(), {content.data(), content.size()});

    auto request = std::make_unique<TEvService::TEvWriteBlocksLocalRequest>();
    request->Record.Sglist = TGuardedSgList(std::move(sglist));
    request->Record.SetStartIndex(range.Start);
    request->Record.BlocksCount = range.Size();
    request->Record.BlockSize = content.size() / range.Size();

    return CreateWriteBlocksHandler(range, std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

TDynBitMap BitMapFromString(const TString& s)
{
    TDynBitMap mask;

    if (s) {
        mask.Reserve(s.size() * 8);
        Y_ABORT_UNLESS(mask.GetChunkCount() * sizeof(TDynBitMap::TChunk) == s.size());
        auto* dst = const_cast<TDynBitMap::TChunk*>(mask.GetChunks());
        memcpy(dst, s.data(), s.size());
    }

    return mask;
}

bool EncryptedBlock(const TDynBitMap& bitmap, size_t block)
{
    return !bitmap.Get(block);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBlockHandlerTest)
{
    Y_UNIT_TEST(ShouldParseIOVector)
    {
        auto blockRange = TBlockRange64::MakeClosedInterval(1, 3);

        auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();

        auto& blocks = *request->Record.MutableBlocks();
        for (ui64 blockIndex = blockRange.Start;
             blockIndex <= blockRange.End;
             ++blockIndex)
        {
            blocks.AddBuffers(GetBlockContent(blockIndex));
        }

        auto handler = CreateWriteBlocksHandler(
            blockRange,
            std::move(request),
            DefaultBlockSize);
        auto guardedSgList = handler->GetBlocks(blockRange);

        auto guard = guardedSgList.Acquire();
        UNIT_ASSERT(guard);
        const auto& sglist = guard.Get();

        for (size_t index = 0; index < sglist.size(); ++index) {
            ui64 blockIndex = blockRange.Start + index;
            UNIT_ASSERT_EQUAL(sglist[index].AsStringBuf(), GetBlockContent(blockIndex));
        }
    }

    Y_UNIT_TEST(ShouldParseIOVectorWithLargeParts)
    {
        auto blockRange = TBlockRange64::MakeClosedInterval(1, 5);

        auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();

        auto& blocks = *request->Record.MutableBlocks();
        blocks.AddBuffers(GetBlockContent(1) + GetBlockContent(2) + GetBlockContent(3));
        blocks.AddBuffers(GetBlockContent(4) + GetBlockContent(5));

        auto handler = CreateWriteBlocksHandler(
            blockRange,
            std::move(request),
            DefaultBlockSize);
        auto guardedSgList = handler->GetBlocks(blockRange);

        auto guard = guardedSgList.Acquire();
        UNIT_ASSERT(guard);
        const auto& sglist = guard.Get();

        for (size_t index = 0; index < sglist.size(); ++index) {
            ui64 blockIndex = blockRange.Start + index;
            UNIT_ASSERT_EQUAL(sglist[index].AsStringBuf(), GetBlockContent(blockIndex));
        }
    }

    Y_UNIT_TEST(ShouldBuildIOVector)
    {
        auto blockRange = TBlockRange64::MakeClosedInterval(1, 3);

        auto handler = CreateReadBlocksHandler(blockRange, DefaultBlockSize);
        for (ui64 blockIndex = blockRange.Start;
             blockIndex <= blockRange.End;
             ++blockIndex)
        {
            auto blockContent = GetBlockContent(blockIndex);
            handler->SetBlock(
                blockIndex,
                TBlockDataRef(blockContent.data(), blockContent.size()),
                false);
        }

        NProto::TReadBlocksResponse response;
        handler->GetResponse(response);

        const auto& blocks = response.GetBlocks();
        UNIT_ASSERT_EQUAL(blocks.BuffersSize(), blockRange.Size());

        for (ui64 blockIndex = blockRange.Start;
             blockIndex <= blockRange.End;
             ++blockIndex)
        {
            UNIT_ASSERT_EQUAL(
                blocks.GetBuffers(blockIndex - blockRange.Start),
                GetBlockContent(blockIndex));
        }
    }

    Y_UNIT_TEST(ShouldSuccessfullySetBlockWhenGuardedSgListIsValid)
    {
        TString s(DefaultBlockSize, char(0));
        TGuardedSgList sglist(TSgList{{s.data(), s.size()}});
        auto handler = CreateReadBlocksHandler(
            TBlockRange64::MakeOneBlock(0),
            sglist,
            DefaultBlockSize);
        UNIT_ASSERT(handler->SetBlock(0, {}, false));
    }

    Y_UNIT_TEST(ShouldFailSetBlockWhenGuardedSgListIsDestroyed)
    {
        TString s(DefaultBlockSize, char(0));
        TGuardedSgList sglist(TSgList{{s.data(), s.size()}});
        auto handler = CreateReadBlocksHandler(
            TBlockRange64::MakeOneBlock(0),
            sglist,
            DefaultBlockSize);
        sglist.Close();
        UNIT_ASSERT(!handler->SetBlock(0, {}, false));
    }

    Y_UNIT_TEST(ShouldZeroMissedBlocksInReadBlocksHandler)
    {
        auto handler = CreateReadBlocksHandler(
            TBlockRange64::WithLength(0, 8),
            DefaultBlockSize);

        TString blockContent1(DefaultBlockSize, 'b');
        handler->SetBlock(
            1,
            TBlockDataRef(blockContent1.data(), blockContent1.size()),
            false);

        auto guardedSgList1 = handler->GetGuardedSgList({4, 5}, false);
        {
            auto guard = guardedSgList1.Acquire();
            UNIT_ASSERT(guard);
            const auto& sgList = guard.Get();
            TString content(DefaultBlockSize, 'd');
            memcpy((char*)sgList[0].Data(), content.data(), content.size());
            memcpy((char*)sgList[1].Data(), content.data(), content.size());
        }
        handler->Clear();

        TString blockContent2(DefaultBlockSize, 'c');
        handler->SetBlock(
            2,
            TBlockDataRef(blockContent2.data(), blockContent2.size()),
            false);

        auto guardedSgList2 = handler->GetGuardedSgList({6, 7}, false);
        {
            auto guard = guardedSgList2.Acquire();
            UNIT_ASSERT(guard);
            const auto& sgList = guard.Get();
            TString content(DefaultBlockSize, 'e');
            memcpy((char*)sgList[0].Data(), content.data(), content.size());
            memcpy((char*)sgList[1].Data(), content.data(), content.size());
        }

        NProto::TReadBlocksResponse response;
        handler->GetResponse(response);

        const auto& Blocks = response.GetBlocks();
        UNIT_ASSERT(Blocks.GetBuffers(0) == TString());
        UNIT_ASSERT(Blocks.GetBuffers(1) == TString());
        UNIT_ASSERT(Blocks.GetBuffers(2) == TString(DefaultBlockSize, 'c'));
        UNIT_ASSERT(Blocks.GetBuffers(3) == TString());
        UNIT_ASSERT(Blocks.GetBuffers(4) == TString());
        UNIT_ASSERT(Blocks.GetBuffers(5) == TString());
        UNIT_ASSERT(Blocks.GetBuffers(6) == TString(DefaultBlockSize, 'e'));
        UNIT_ASSERT(Blocks.GetBuffers(7) == TString(DefaultBlockSize, 'e'));
        UNIT_ASSERT(!response.GetAllZeroes());
    }

    Y_UNIT_TEST(ShouldZeroMissedBlocksInLocalReadBlocksHandler)
    {
        TVector<TString> blocks;
        auto sgList = ResizeBlocks(blocks, 8, TString(DefaultBlockSize, 'a'));

        auto handler = CreateReadBlocksHandler(
            TBlockRange64::WithLength(0, blocks.size()),
            TGuardedSgList(std::move(sgList)),
            DefaultBlockSize);

        TString blockContent1(DefaultBlockSize, 'b');
        handler->SetBlock(
            1,
            TBlockDataRef(blockContent1.data(), blockContent1.size()),
            false);

        auto guardedSgList1 = handler->GetGuardedSgList({4, 5}, false);
        {
            auto guard = guardedSgList1.Acquire();
            UNIT_ASSERT(guard);
            const auto& sgList = guard.Get();
            TString content(DefaultBlockSize, 'd');
            memcpy((char*)sgList[0].Data(), content.data(), content.size());
            memcpy((char*)sgList[1].Data(), content.data(), content.size());
        }
        handler->Clear();

        TString blockContent2(DefaultBlockSize, 'c');
        handler->SetBlock(
            2,
            TBlockDataRef(blockContent2.data(), blockContent2.size()),
            false);

        auto guardedSgList2 = handler->GetGuardedSgList({6, 7}, false);
        {
            auto guard = guardedSgList2.Acquire();
            UNIT_ASSERT(guard);
            const auto& sgList = guard.Get();
            TString content(DefaultBlockSize, 'e');
            memcpy((char*)sgList[0].Data(), content.data(), content.size());
            memcpy((char*)sgList[1].Data(), content.data(), content.size());
        }

        UNIT_ASSERT(blocks[0] == TString(DefaultBlockSize, 'a'));
        UNIT_ASSERT(blocks[3] == TString(DefaultBlockSize, 'a'));

        NProto::TReadBlocksLocalResponse response;
        auto responseSgList = handler->GetLocalResponse(response);
        {
            auto guard = responseSgList.Acquire();
            UNIT_ASSERT(guard);
            auto resBlocks = guard.Get();
            UNIT_ASSERT(resBlocks[0].AsStringBuf() == TString(DefaultBlockSize, char(0)));
            UNIT_ASSERT(resBlocks[1].AsStringBuf() == TString(DefaultBlockSize, char(0)));
            UNIT_ASSERT(resBlocks[2].AsStringBuf() == TString(DefaultBlockSize, 'c'));
            UNIT_ASSERT(resBlocks[3].AsStringBuf() == TString(DefaultBlockSize, char(0)));
            UNIT_ASSERT(resBlocks[4].AsStringBuf() == TString(DefaultBlockSize, char(0)));
            UNIT_ASSERT(resBlocks[5].AsStringBuf() == TString(DefaultBlockSize, char(0)));
            UNIT_ASSERT(resBlocks[6].AsStringBuf() == TString(DefaultBlockSize, 'e'));
            UNIT_ASSERT(resBlocks[7].AsStringBuf() == TString(DefaultBlockSize, 'e'));
        }
        UNIT_ASSERT(!response.GetAllZeroes());
    }

    Y_UNIT_TEST(ShouldSetAllZeroesFlagInReadBlocksResponse)
    {
        auto handler = CreateReadBlocksHandler(
            TBlockRange64::WithLength(0, 8),
            DefaultBlockSize);

        auto zeroBlock = TBlockDataRef::CreateZeroBlock(DefaultBlockSize);
        handler->SetBlock(0, zeroBlock, false);
        handler->SetBlock(1, zeroBlock, true);

        NProto::TReadBlocksResponse response;
        handler->GetResponse(response);
        UNIT_ASSERT(response.GetAllZeroes());
        for (const auto& buffer: response.GetBlocks().GetBuffers()) {
            UNIT_ASSERT(buffer == TString());
        }

        auto bitmap = BitMapFromString(response.GetUnencryptedBlockMask());
        UNIT_ASSERT(!EncryptedBlock(bitmap, 0));
        UNIT_ASSERT(!EncryptedBlock(bitmap, 1));
    }

    Y_UNIT_TEST(ShouldSetAllZeroesFlagInReadBlocksLocalResponse)
    {
        TVector<TString> blocks;
        auto sgList = ResizeBlocks(blocks, 8, TString(DefaultBlockSize, 'a'));

        auto handler = CreateReadBlocksHandler(
            TBlockRange64::WithLength(0, blocks.size()),
            TGuardedSgList(std::move(sgList)),
            DefaultBlockSize);

        auto zeroBlock = TBlockDataRef::CreateZeroBlock(DefaultBlockSize);
        handler->SetBlock(0, zeroBlock, false);
        handler->SetBlock(1, zeroBlock, true);

        NProto::TReadBlocksLocalResponse response;
        auto responseSgList = handler->GetLocalResponse(response);
        UNIT_ASSERT(response.GetAllZeroes());
        auto guard = responseSgList.Acquire();
        UNIT_ASSERT(guard);
        for (const auto& buffer: guard.Get()) {
            UNIT_ASSERT(
                buffer.AsStringBuf() == TString(DefaultBlockSize, char(0)));
        }
    }

    Y_UNIT_TEST(ShouldMixWriteBlocksHandlers)
    {
        TVector<TString> holders;

        TVector<std::pair<IWriteBlocksHandlerPtr, TBlockRange64>> parts;
        auto addPart = [&] (TBlockRange64 range, char data) {
            TString content(DefaultBlockSize, data);
            auto handler = CreateTestWriteBlocksHandler(range, content);

            parts.emplace_back(std::move(handler), range);
            holders.push_back(std::move(content));
        };

        addPart(TBlockRange64::MakeClosedInterval(0, 1), 'a');
        addPart(TBlockRange64::MakeClosedInterval(1, 3), 'b');
        addPart(TBlockRange64::MakeClosedInterval(3, 4), 'c');
        addPart(TBlockRange64::MakeClosedInterval(5, 6), 'd');
        addPart(TBlockRange64::MakeClosedInterval(7, 7), 'e');

        auto mixedHandler = CreateMixedWriteBlocksHandler(parts);
        auto sglist = mixedHandler->GetBlocks(TBlockRange64::WithLength(0, 8));
        auto guard = sglist.Acquire();
        UNIT_ASSERT(guard);
        auto resBlocks = guard.Get();

        UNIT_ASSERT(resBlocks.size() == 8);
        UNIT_ASSERT(resBlocks[0].AsStringBuf() == TString(DefaultBlockSize, 'a'));
        UNIT_ASSERT(resBlocks[1].AsStringBuf() == TString(DefaultBlockSize, 'b'));
        UNIT_ASSERT(resBlocks[2].AsStringBuf() == TString(DefaultBlockSize, 'b'));
        UNIT_ASSERT(resBlocks[3].AsStringBuf() == TString(DefaultBlockSize, 'c'));
        UNIT_ASSERT(resBlocks[4].AsStringBuf() == TString(DefaultBlockSize, 'c'));
        UNIT_ASSERT(resBlocks[5].AsStringBuf() == TString(DefaultBlockSize, 'd'));
        UNIT_ASSERT(resBlocks[6].AsStringBuf() == TString(DefaultBlockSize, 'd'));
        UNIT_ASSERT(resBlocks[7].AsStringBuf() == TString(DefaultBlockSize, 'e'));
    }

    Y_UNIT_TEST(ShouldDestroySgListInWriteBlocksDtor)
    {
        auto blockRange = TBlockRange64::MakeClosedInterval(1, 3);
        auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();
        auto& blocks = *request->Record.MutableBlocks();

        for (ui64 i = blockRange.Start; i <= blockRange.End; ++i) {
            blocks.AddBuffers(GetBlockContent(i));
        }

        auto handler = CreateWriteBlocksHandler(
            blockRange,
            std::move(request),
            DefaultBlockSize);
        auto guardedSgList = handler->GetBlocks(blockRange);

        {
            auto guard = guardedSgList.Acquire();
            UNIT_ASSERT(guard);
        }

        handler.reset();
        auto guard = guardedSgList.Acquire();
        UNIT_ASSERT(!guard);
    }

    Y_UNIT_TEST(ShouldDestroySgListInReadBlocksDtor)
    {
        auto handler = CreateReadBlocksHandler(
            TBlockRange64::WithLength(0, 8),
            DefaultBlockSize);
        auto guardedSgList = handler->GetGuardedSgList({4, 5}, false);

        {
            auto guard = guardedSgList.Acquire();
            UNIT_ASSERT(guard);
        }

        handler.reset();
        auto guard = guardedSgList.Acquire();
        UNIT_ASSERT(!guard);
    }

    Y_UNIT_TEST(ShouldFillUnencryptedBlockMaskInReadBlocksHandler)
    {
        auto blockContent = GetBlockContent('a');
        TBlockDataRef blockContentRef(blockContent.data(), blockContent.size());

        ui64 startIndex = 10;
        auto handler = CreateReadBlocksHandler(
            TBlockRange64::WithLength(startIndex, 7),
            DefaultBlockSize);

        auto zeroBlock = TBlockDataRef::CreateZeroBlock(DefaultBlockSize);
        handler->SetBlock(startIndex + 0, zeroBlock, false);
        handler->SetBlock(startIndex + 1, zeroBlock, true);

        handler->SetBlock(startIndex + 2, blockContentRef, false);
        handler->SetBlock(startIndex + 3, blockContentRef, true);

        handler->GetGuardedSgList({startIndex + 4}, false);
        handler->GetGuardedSgList({startIndex + 5}, true);

        NProto::TReadBlocksResponse response;
        handler->GetResponse(response);
        auto bitmap = BitMapFromString(response.GetUnencryptedBlockMask());

        UNIT_ASSERT(!EncryptedBlock(bitmap, 0));
        UNIT_ASSERT(!EncryptedBlock(bitmap, 1));

        UNIT_ASSERT(EncryptedBlock(bitmap, 2));
        UNIT_ASSERT(!EncryptedBlock(bitmap, 3));

        UNIT_ASSERT(EncryptedBlock(bitmap, 4));
        UNIT_ASSERT(!EncryptedBlock(bitmap, 5));

        UNIT_ASSERT(!EncryptedBlock(bitmap, 6));
        UNIT_ASSERT(bitmap.Get(7) == 0);
    }

    Y_UNIT_TEST(ShouldFillUnencryptedBlockMaskInLocalReadBlocksHandler)
    {
        auto blockContent = GetBlockContent();
        TBlockDataRef blockContentRef(blockContent.data(), blockContent.size());

        ui64 startIndex = 10;
        TVector<TString> blocks;
        auto sgList = ResizeBlocks(blocks, 7, GetBlockContent());

        auto handler = CreateReadBlocksHandler(
            TBlockRange64::WithLength(startIndex, blocks.size()),
            TGuardedSgList(std::move(sgList)),
            DefaultBlockSize);

        auto zeroBlock = TBlockDataRef::CreateZeroBlock(DefaultBlockSize);
        handler->SetBlock(startIndex + 0, zeroBlock, false);
        handler->SetBlock(startIndex + 1, zeroBlock, true);

        handler->SetBlock(startIndex + 2, blockContentRef, false);
        handler->SetBlock(startIndex + 3, blockContentRef, true);

        handler->GetGuardedSgList({startIndex + 4}, false);
        handler->GetGuardedSgList({startIndex + 5}, true);

        NProto::TReadBlocksLocalResponse response;
        handler->GetLocalResponse(response);
        auto bitmap = BitMapFromString(response.GetUnencryptedBlockMask());

        UNIT_ASSERT(!EncryptedBlock(bitmap, 0));
        UNIT_ASSERT(!EncryptedBlock(bitmap, 1));

        UNIT_ASSERT(EncryptedBlock(bitmap, 2));
        UNIT_ASSERT(!EncryptedBlock(bitmap, 3));

        UNIT_ASSERT(EncryptedBlock(bitmap, 4));
        UNIT_ASSERT(!EncryptedBlock(bitmap, 5));

        UNIT_ASSERT(!EncryptedBlock(bitmap, 6));
        UNIT_ASSERT(bitmap.Get(7) == 0);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
