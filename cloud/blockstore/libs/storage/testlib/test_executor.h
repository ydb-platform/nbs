#pragma once

#include <cloud/blockstore/libs/storage/model/public.h>

#include <cloud/storage/core/libs/tablet/blob_id.h>
#include <cloud/storage/core/libs/tablet/model/commit.h>
#include <cloud/storage/core/libs/tablet/model/partial_blob_id.h>

#include <ydb/core/tablet_flat/flat_database.h>
#include <ydb/core/tablet_flat/test/libs/table/test_dummy.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TTestExecutor
{
    static const ui64 TabletId = 0;
    static const ui32 Channel = 0;
    static const ui32 Gen = 0;
    ui32 Step = 0;
    ui32 Cookie = 0;

    NKikimr::NTable::TDatabase DB;

    template <typename T>
    ui64 WriteTx(T&& action)
    {
        NKikimr::NTable::TDummyEnv env;
        DB.Begin(++Step, env);
        Cookie = 0;
        action(DB);
        return DB.Commit(Step, true).Change->Stamp;
    }

    template <typename T>
    ui64 ReadTx(T&& action)
    {
        NKikimr::NTable::TDummyEnv env;
        DB.Begin(Step, env);
        action(DB);
        return DB.Commit(Step, false).Change->Stamp;
    }

    ui64 CommitId() const
    {
        return MakeCommitId(Gen, Step);
    }

    TPartialBlobId MakeBlobId(ui32 blocksCount = MaxBlocksCount)
    {
        return TPartialBlobId(
            Gen,
            Step,
            Channel,
            blocksCount * DefaultBlockSize,
            Cookie++,
            0);
    }
};

}   // namespace NCloud::NBlockStore::NStorage
