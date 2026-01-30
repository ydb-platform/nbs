#include "server_memory_state.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>

namespace NCloud::NFileStore::NServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestEnv
{
    TTempDir TempDir;

    TString GetBasePath() const
    {
        return TempDir.Path();
    }

    TString CreateTestFile(const TString& relativePath, size_t size)
    {
        TFsPath fullPath = TFsPath(TempDir.Path()).Child(relativePath);
        fullPath.Parent().MkDirs();

        TFile file(fullPath.GetPath(), CreateAlways | WrOnly);
        file.Resize(size);
        file.Close();

        return relativePath;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServerStateTest)
{
    Y_UNIT_TEST(ShouldCreateMmapRegion)
    {
        TTestEnv env;
        TServerState state(env.GetBasePath());

        const size_t fileSize = 4096;
        TString relativePath = env.CreateTestFile("test_file.dat", fileSize);

        auto result = state.CreateMmapRegion(relativePath, fileSize);

        UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));

        auto mmapInfo = result.ExtractResult();
        UNIT_ASSERT(mmapInfo.Address != nullptr);
        UNIT_ASSERT(mmapInfo.Id != 0);
        UNIT_ASSERT_GT(mmapInfo.LatestActivityTimestamp, TInstant());

        auto regions = state.ListMmapRegions();
        UNIT_ASSERT_VALUES_EQUAL(1u, regions.size());

        const auto mmapInfoListed = regions[0];
        UNIT_ASSERT_VALUES_EQUAL(mmapInfo.FilePath, mmapInfoListed.FilePath);
        UNIT_ASSERT_VALUES_EQUAL(mmapInfo.Size, mmapInfoListed.Size);
        UNIT_ASSERT_VALUES_EQUAL(mmapInfo.Id, mmapInfoListed.Id);
        UNIT_ASSERT_VALUES_EQUAL(
            mmapInfo.LatestActivityTimestamp,
            mmapInfoListed.LatestActivityTimestamp);

        const auto mmapInfoById = state.GetMmapRegion(mmapInfo.Id);
        UNIT_ASSERT_C(
            !HasError(mmapInfoById),
            FormatError(mmapInfoById.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(
            mmapInfo.FilePath,
            mmapInfoById.GetResult().FilePath);
        UNIT_ASSERT_VALUES_EQUAL(mmapInfo.Size, mmapInfoById.GetResult().Size);
        UNIT_ASSERT_VALUES_EQUAL(mmapInfo.Id, mmapInfoById.GetResult().Id);
        UNIT_ASSERT_VALUES_EQUAL(
            mmapInfo.LatestActivityTimestamp,
            mmapInfoById.GetResult().LatestActivityTimestamp);

        // validate that the process has indeed mmaped the file
        char* data = static_cast<char*>(mmapInfo.Address);
        const TString expectedData = "Hello, world!";
        ::memcpy(data, expectedData.data(), expectedData.size());

        // unmmap before reading from the file
        result = state.DestroyMmapRegion(mmapInfo.Id);
        UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));

        // read back from the file
        TFile file(
            TFsPath(env.GetBasePath()).Child("test_file.dat").GetPath(),
            OpenExisting | RdOnly);
        TString readData(expectedData.size(), '\0');
        file.Read((void*)(readData.data()), readData.size());
        UNIT_ASSERT_VALUES_EQUAL(expectedData, readData);
    }

    Y_UNIT_TEST(ShouldFailCreateMmapRegionWithEdgeCases)
    {
        TTestEnv env;
        TServerState state(env.GetBasePath());

        TString relativePath = env.CreateTestFile("test_file.dat", 4096);

        // zero size
        auto result = state.CreateMmapRegion(relativePath, 0);
        UNIT_ASSERT(HasError(result));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, result.GetError().GetCode());

        // size greater than file size
        result = state.CreateMmapRegion(relativePath, 8192);
        UNIT_ASSERT(HasError(result));
        UNIT_ASSERT_VALUES_EQUAL(E_IO, result.GetError().GetCode());

        // absolute path instead of relative
        TString absolutePath =
            TFsPath(env.GetBasePath()).Child("test_file.dat").GetPath();
        result = state.CreateMmapRegion(absolutePath, 4096);
        UNIT_ASSERT(HasError(result));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, result.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldCreateMmapRegionForNestedPath)
    {
        TTestEnv env;
        TServerState state(env.GetBasePath());

        const size_t fileSize = 4096;
        TString relativePath = env.CreateTestFile(JoinFsPaths(
            "nested",
            "path",
            "to",
            "test_file.dat"), fileSize);

        auto result = state.CreateMmapRegion(relativePath, fileSize);

        UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));

        auto mmapInfo = result.ExtractResult();
        UNIT_ASSERT(mmapInfo.Address != nullptr);
        UNIT_ASSERT(mmapInfo.Id != 0);
        UNIT_ASSERT_GT(mmapInfo.LatestActivityTimestamp, TInstant());
    }

    Y_UNIT_TEST(ShouldFailCreateMmapRegionForNonExistentFile)
    {
        TTestEnv env;
        TServerState state(env.GetBasePath());

        auto result = state.CreateMmapRegion("nonexistent.dat", 4096);

        UNIT_ASSERT(HasError(result));
    }

    Y_UNIT_TEST(ShouldDestroyMmapRegion)
    {
        TTestEnv env;
        TServerState state(env.GetBasePath());

        const size_t fileSize = 4096;
        TString relativePath = env.CreateTestFile("test_file.dat", fileSize);

        auto createResult = state.CreateMmapRegion(relativePath, fileSize);
        UNIT_ASSERT_C(
            !HasError(createResult),
            FormatError(createResult.GetError()));

        const auto mmapInfo = createResult.ExtractResult();

        auto regions = state.ListMmapRegions();
        UNIT_ASSERT_VALUES_EQUAL(1u, regions.size());

        auto destroyResult = state.DestroyMmapRegion(mmapInfo.Id);
        UNIT_ASSERT_C(!HasError(destroyResult), FormatError(destroyResult));

        regions = state.ListMmapRegions();
        UNIT_ASSERT_VALUES_EQUAL(0u, regions.size());

        const auto result = state.DestroyMmapRegion(999999);

        UNIT_ASSERT(HasError(result));
        UNIT_ASSERT_VALUES_EQUAL(E_TRANSPORT_ERROR, result.GetCode());
    }

    Y_UNIT_TEST(ShouldListMmapRegions)
    {
        TTestEnv env;
        TServerState state(env.GetBasePath());

        auto regions = state.ListMmapRegions();
        UNIT_ASSERT_VALUES_EQUAL(0u, regions.size());

        TString path1 = env.CreateTestFile("file1.dat", 4096);
        TString path2 = env.CreateTestFile("file2.dat", 8192);

        auto result1 = state.CreateMmapRegion(path1, 4096);
        auto result2 = state.CreateMmapRegion(path2, 8192);

        UNIT_ASSERT_C(!HasError(result1), FormatError(result1.GetError()));
        UNIT_ASSERT_C(!HasError(result2), FormatError(result2.GetError()));

        regions = state.ListMmapRegions();
        // sorting to have a deterministic order
        Sort(regions.begin(), regions.end());
        Sort(regions.begin(), regions.end());

        UNIT_ASSERT_VALUES_EQUAL(2u, regions.size());
        UNIT_ASSERT_VALUES_EQUAL(
            result1.GetResult().FilePath,
            regions[0].FilePath);
        UNIT_ASSERT_VALUES_EQUAL(result1.GetResult().Size, regions[0].Size);
        UNIT_ASSERT_VALUES_EQUAL(result1.GetResult().Id, regions[0].Id);
        UNIT_ASSERT_VALUES_EQUAL(
            result2.GetResult().FilePath,
            regions[1].FilePath);
        UNIT_ASSERT_VALUES_EQUAL(result2.GetResult().Size, regions[1].Size);
        UNIT_ASSERT_VALUES_EQUAL(result2.GetResult().Id, regions[1].Id);
    }

    Y_UNIT_TEST(ShouldGetMmapRegion)
    {
        TTestEnv env;
        TServerState state(env.GetBasePath());

        const size_t fileSize = 4096;
        TString relativePath = env.CreateTestFile("test_file.dat", fileSize);

        auto result = state.CreateMmapRegion(relativePath, fileSize);
        UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));

        const auto mmapInfo = result.ExtractResult();

        result = state.GetMmapRegion(mmapInfo.Id);
        UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(
            mmapInfo.FilePath,
            result.GetResult().FilePath);
        UNIT_ASSERT_VALUES_EQUAL(mmapInfo.Size, result.GetResult().Size);
        UNIT_ASSERT_VALUES_EQUAL(mmapInfo.Id, result.GetResult().Id);

        result = state.GetMmapRegion(mmapInfo.Id + 1);
        UNIT_ASSERT(HasError(result));
        UNIT_ASSERT_VALUES_EQUAL(
            E_TRANSPORT_ERROR,
            result.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldPingMmapRegion)
    {
        TTestEnv env;
        TServerState state(env.GetBasePath());

        const size_t fileSize = 4096;
        TString relativePath = env.CreateTestFile("test_file.dat", fileSize);

        auto result = state.CreateMmapRegion(relativePath, fileSize);
        UNIT_ASSERT_C(!HasError(result), FormatError(result.GetError()));

        const auto mmapInfo = result.ExtractResult();
        const auto oldTimestamp = mmapInfo.LatestActivityTimestamp;

        // wait a bit to ensure timestamp difference
        Sleep(TDuration::MilliSeconds(10));

        auto pingResult = state.PingMmapRegion(mmapInfo.Id);
        UNIT_ASSERT_C(!HasError(pingResult), FormatError(pingResult));

        auto getResult = state.GetMmapRegion(mmapInfo.Id);
        UNIT_ASSERT(!HasError(getResult));
        UNIT_ASSERT_GT(
            getResult.GetResult().LatestActivityTimestamp,
            oldTimestamp);
    }
}

}   // namespace NCloud::NFileStore::NServer
