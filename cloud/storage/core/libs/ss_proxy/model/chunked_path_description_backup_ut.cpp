#include "chunked_path_description_backup.h"

#include <cloud/blockstore/libs/storage/api/partition.h>
#include <cloud/blockstore/libs/storage/core/config.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/tempfile.h>

using namespace NCloud::NStorage::NSSProxy;

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

void GenerateData(
    size_t count,
    NCloud::NStorage::NSSProxy::NProto::TPathDescriptionBackup* backup)
{
    auto& backupData = *backup->MutableData();
    for (size_t i = 0; i < count; ++i) {
        TString path = "/Root/NBS/vol_" + std::to_string(i);
        auto& item = backupData[path];
        auto& self = *item.MutableSelf();
        self.SetName("vol_" + std::to_string(i));
    }
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TChunkedPathDescriptionBackupTest)
{
    void DoShouldSaveAndLoad(size_t count)
    {
        TTempFileHandle file;

        NCloud::NStorage::NSSProxy::NProto::TPathDescriptionBackup savedBackup;
        {
            GenerateData(count, &savedBackup);

            auto result = SavePathDescriptionBackupToChunkedBinaryFormat(
                file.Name(),
                savedBackup);

            UNIT_ASSERT_EQUAL_C(S_OK, result.GetCode(), FormatError(result));
        }

        NCloud::NStorage::NSSProxy::NProto::TPathDescriptionBackup loadedBackup;
        {
            auto result = LoadPathDescriptionBackupFromChunkedBinaryFormat(
                file.Name(),
                &loadedBackup);
            UNIT_ASSERT_EQUAL_C(S_OK, result.GetCode(), FormatError(result));
            UNIT_ASSERT_EQUAL(
                savedBackup.GetData().size(),
                loadedBackup.GetData().size());
            for (const auto& [path, item]: savedBackup.GetData()) {
                const auto& loadedItem =
                    loadedBackup.GetData().find(path)->second;
                UNIT_ASSERT_VALUES_EQUAL(
                    item.DebugString(),
                    loadedItem.DebugString());
            }
        }
    }

    Y_UNIT_TEST(ShouldSaveAndLoadEmpty)
    {
        DoShouldSaveAndLoad(0);
    }

    Y_UNIT_TEST(ShouldSaveAndLoadOneChunk)
    {
        DoShouldSaveAndLoad(100);
    }

    Y_UNIT_TEST(ShouldSaveAnLoadSeveralChunks)
    {
        DoShouldSaveAndLoad(3500);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
