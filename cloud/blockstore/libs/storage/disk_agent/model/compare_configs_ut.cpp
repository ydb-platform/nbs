#include "compare_configs.h"

#include "public.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/system/file.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TFixture
    : public NUnitTest::TBaseFixture
{
    const TTempDir TempDir;

    NProto::TFileDeviceArgs File0;
    NProto::TFileDeviceArgs File1;
    NProto::TFileDeviceArgs File2;
    NProto::TFileDeviceArgs File3;
    NProto::TFileDeviceArgs NonExistent1;
    NProto::TFileDeviceArgs NonExistent2;

    void SetUp(NUnitTest::TTestContext& /*context*/) override
    {
        File0.SetDeviceId("0");
        File0.SetPath(CreatePath("NVME01"));
        File0.SetPoolName("");
        File0.SetBlockSize(4096);
        File0.SetOffset(1000);
        File0.SetFileSize(1000000);

        NonExistent1.SetDeviceId("1");
        NonExistent1.SetPath("NonExistent1");
        NonExistent1.SetBlockSize(4096);
        NonExistent1.SetOffset(100000);
        NonExistent1.SetFileSize(2000000);

        File1.SetDeviceId("2");
        File1.SetPath(CreatePath("NVME02"));
        File1.SetPoolName("local");
        File1.SetBlockSize(512);
        File1.SetOffset(10000);
        File1.SetFileSize(0);

        NonExistent2.SetDeviceId("3");
        NonExistent2.SetPath("NonExistent2");
        NonExistent2.SetBlockSize(4096);
        NonExistent2.SetOffset(100000);
        NonExistent2.SetFileSize(2000000);

        File2.SetDeviceId("4");
        File2.SetPath(CreatePath("ROT01"));
        File2.SetPoolName("rot");
        File2.SetBlockSize(4096);
        File2.SetOffset(100000);
        File2.SetFileSize(1000000);

        File3.SetDeviceId("5");
        File3.SetPath(CreatePath("ROT02"));
        File3.SetPoolName("rot");
        File3.SetBlockSize(4096);
        File3.SetOffset(100000);
        File3.SetFileSize(2000000);
    }

    TString CreatePath(const TString& name)
    {
        TString path = TempDir.Path() / name;

        TFile file(path, EOpenModeFlag::CreateNew);

        return path;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCompareConfigsTest)
{
    Y_UNIT_TEST_F(ShouldAcceptEmptyConfigs, TFixture)
    {
        {
            const auto error = CompareConfigs({}, {});
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
        }

        {
            const auto error = CompareConfigs({}, {File0});
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
        }

        {
            const auto error = CompareConfigs({File0}, {});
            UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(), error);
        }
    }

    Y_UNIT_TEST_F(ShouldRejectUnsortedConfigs, TFixture)
    {
        {
            const auto error = CompareConfigs({File1, File0}, {File0, File1});
            UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(), error);
        }

        {
            const auto error = CompareConfigs({File0, File1}, {File1, File0});
            UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(), error);
        }

        {
            const auto error =
                CompareConfigs({File0, File1}, {File2, File0, File1});
            UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(), error);
        }

        {
            const auto error = CompareConfigs({File0, File1}, {File0, File1});
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
        }
    }

    Y_UNIT_TEST_F(ShouldDetectLostDevices, TFixture)
    {
        {
            const auto error = CompareConfigs({File1}, {File2});
            UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(), error);
        }

        {
            const auto error = CompareConfigs({File1}, {File0});
            UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(), error);
        }

        {
            const auto error = CompareConfigs(
                {File0, File1, File2},
                {File0, File2, File3});
            UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(), error);
        }

        {
            const auto error =
                CompareConfigs({File0, NonExistent1}, {File0, File1});
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
        }

        {
            const auto error = CompareConfigs(
                {File0, NonExistent1, File1, NonExistent2, File2},
                {File0, File1, File2, File3});
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
        }

        {
            const auto error = CompareConfigs(
                {File0, NonExistent1, File1, NonExistent2, File2},
                {File0, File2, File3});
            UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(), error);
        }
    }

    Y_UNIT_TEST_F(ShouldAllowNewDevices, TFixture)
    {
        const auto error = CompareConfigs(
            {File2, File3},
            {File0, File1, File2, File3});

        UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
    }

    Y_UNIT_TEST_F(ShouldDetectConfigChanges, TFixture)
    {
        auto compare = [&] (auto file1) {
            return CompareConfigs(
                {File0, File1, File2, File3},
                {File0, file1, File2, File3});
        };

        {
            auto file1 = File1;
            file1.SetPath("foo");

            const auto error = compare(file1);
            UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(), error);
        }

        {
            auto file1 = File1;
            file1.SetPoolName("foo");

            const auto error = compare(file1);
            UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(), error);
        }

        {
            auto file1 = File1;
            file1.SetBlockSize(1024);

            const auto error = compare(file1);
            UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(), error);
        }

        {
            auto file1 = File1;
            file1.SetOffset(42);

            const auto error = compare(file1);
            UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(), error);
        }

        {
            auto file1 = File1;
            file1.SetFileSize(42);

            const auto error = CompareConfigs(
                {File0, File1, File2, File3},
                {File0, file1, File2, File3});
            UNIT_ASSERT_VALUES_EQUAL_C(S_OK, error.GetCode(), error);
            UNIT_ASSERT_VALUES_EQUAL(0, File1.GetFileSize());
        }

        {
            auto file2 = File2;
            file2.SetFileSize(42);

            const auto error = CompareConfigs(
                {File0, File1, File2, File3},
                {File0, File1, file2, File3});
            UNIT_ASSERT_VALUES_EQUAL_C(E_ARGUMENT, error.GetCode(), error);
            UNIT_ASSERT_VALUES_UNEQUAL(0, File2.GetFileSize());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
