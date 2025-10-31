#include "helpers.h"

#include <cloud/filestore/public/api/protos/fs.pb.h>

#include <cloud/storage/core/protos/media.pb.h>

#include <google/protobuf/util/message_differencer.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/protos/filestore_config.pb.h>

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
}

}   // namespace NCloud::NFileStore::NStorage
