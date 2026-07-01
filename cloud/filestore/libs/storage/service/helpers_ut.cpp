#include "helpers.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <cloud/storage/core/protos/media.pb.h>

#include <google/protobuf/util/message_differencer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <contrib/ydb/core/protos/filestore_config.pb.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TConfigs
    : public NUnitTest::TBaseFixture
{
    NProto::TFileStore TargetFileStore;

    NKikimrFileStore::TConfig SrcConfig;

    TString ReportDiff;
    google::protobuf::io::StringOutputStream Stream;
    google::protobuf::util::MessageDifferencer Comparator;
    google::protobuf::util::MessageDifferencer::StreamReporter Reporter;

    TConfigs()
        : Stream(&ReportDiff)
        , Reporter(&Stream)
    {}

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        Comparator.set_report_matches(false);
        Comparator.set_report_moves(false);

        Comparator.ReportDifferencesTo(&Reporter);

        // Setup TConfig
        SrcConfig.SetFileSystemId("test_fs");
        SrcConfig.SetProjectId("test_project");
        SrcConfig.SetFolderId("test_folder");
        SrcConfig.SetCloudId("test_cloud");
        SrcConfig.SetBlockSize(1);
        SrcConfig.SetBlocksCount(2);
        SrcConfig.SetVersion(3);
        SrcConfig.SetNodesCount(4);
        SrcConfig.SetStorageMediaKind(NCloud::NProto::STORAGE_MEDIA_SSD);

        TargetFileStore.SetFileSystemId(SrcConfig.GetFileSystemId());
        TargetFileStore.SetProjectId(SrcConfig.GetProjectId());
        TargetFileStore.SetFolderId(SrcConfig.GetFolderId());
        TargetFileStore.SetCloudId(SrcConfig.GetCloudId());
        TargetFileStore.SetBlockSize(SrcConfig.GetBlockSize());
        TargetFileStore.SetBlocksCount(SrcConfig.GetBlocksCount());
        TargetFileStore.SetConfigVersion(SrcConfig.GetVersion());
        TargetFileStore.SetNodesCount(SrcConfig.GetNodesCount());
        TargetFileStore.SetStorageMediaKind(SrcConfig.GetStorageMediaKind());
    }

    void TearDown(NUnitTest::TTestContext& /*context*/) override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(THelpers)
{
    Y_UNIT_TEST_F(ShouldCorrectlyConvertConfigToFileStore, TConfigs)
    {
        NProto::TFileStore fileStore;
        Convert(SrcConfig, fileStore);

        Comparator.Compare(TargetFileStore, fileStore);
        UNIT_ASSERT_VALUES_EQUAL("", ReportDiff);
    }

    Y_UNIT_TEST(ShouldTreatEmptyInplaceBitmapAsAllZeroes)
    {
        TString data;

        const TInplaceBitmap bitmap(data);

        UNIT_ASSERT(!bitmap.Get(0));
        UNIT_ASSERT(!bitmap.Get(1));
        UNIT_ASSERT(!bitmap.Get(7));
        UNIT_ASSERT(!bitmap.Get(8));
        UNIT_ASSERT(!bitmap.Get(63));
        UNIT_ASSERT(!bitmap.Get(64));
        UNIT_ASSERT(!bitmap.Get(100'000));
    }

    Y_UNIT_TEST(ShouldSetAndReadInplaceBitmapBits)
    {
        TString data;
        TMutableInplaceBitmap bitmap(data);

        bitmap.Set(0);
        UNIT_ASSERT_VALUES_EQUAL(1, data.size());
        UNIT_ASSERT_VALUES_EQUAL(0x01, static_cast<ui8>(data[0]));

        bitmap.Set(7);
        UNIT_ASSERT_VALUES_EQUAL(1, data.size());
        UNIT_ASSERT_VALUES_EQUAL(0x81, static_cast<ui8>(data[0]));

        bitmap.Set(8);
        UNIT_ASSERT_VALUES_EQUAL(2, data.size());
        UNIT_ASSERT_VALUES_EQUAL(0x01, static_cast<ui8>(data[1]));

        bitmap.Set(63);
        UNIT_ASSERT_VALUES_EQUAL(8, data.size());
        UNIT_ASSERT_VALUES_EQUAL(0x80, static_cast<ui8>(data[7]));

        bitmap.Set(64);
        bitmap.Set(65);
        UNIT_ASSERT_VALUES_EQUAL(9, data.size());
        UNIT_ASSERT_VALUES_EQUAL(0x03, static_cast<ui8>(data[8]));

        UNIT_ASSERT(bitmap.Get(0));
        UNIT_ASSERT(bitmap.Get(7));
        UNIT_ASSERT(bitmap.Get(8));
        UNIT_ASSERT(bitmap.Get(63));
        UNIT_ASSERT(bitmap.Get(64));
        UNIT_ASSERT(bitmap.Get(65));

        UNIT_ASSERT(!bitmap.Get(1));
        UNIT_ASSERT(!bitmap.Get(6));
        UNIT_ASSERT(!bitmap.Get(9));
        UNIT_ASSERT(!bitmap.Get(62));
        UNIT_ASSERT(!bitmap.Get(66));
    }

    Y_UNIT_TEST(ShouldExposeMutableInplaceBitmapChangesViaReadonlyView)
    {
        TString data;
        const TInplaceBitmap readonlyBitmap(data);
        TMutableInplaceBitmap mutableBitmap(data);

        UNIT_ASSERT(!readonlyBitmap.Get(25));

        mutableBitmap.Set(25);

        UNIT_ASSERT(readonlyBitmap.Get(25));
        UNIT_ASSERT(mutableBitmap.Get(25));
    }

    Y_UNIT_TEST(ShouldClearInplaceBitmap)
    {
        TString data;
        TMutableInplaceBitmap bitmap(data);

        bitmap.Set(3);
        bitmap.Set(100);

        UNIT_ASSERT(!data.empty());
        UNIT_ASSERT(bitmap.Get(3));
        UNIT_ASSERT(bitmap.Get(100));

        bitmap.Clear();

        UNIT_ASSERT(data.empty());
        UNIT_ASSERT(!bitmap.Get(3));
        UNIT_ASSERT(!bitmap.Get(100));

        bitmap.Set(5);

        UNIT_ASSERT_VALUES_EQUAL(1, data.size());
        UNIT_ASSERT(bitmap.Get(5));
    }
}

}   // namespace NCloud::NFileStore::NStorage
