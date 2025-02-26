#include "file_io_service_provider.h"

#include <cloud/storage/core/libs/common/file_io_service.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestFileIOService
    : IFileIOService
{
    int Started = 0;
    int Stopped = 0;

    void Start() override
    {
        ++Started;
    }

    void Stop() override
    {
        ++Stopped;
    }

    void AsyncRead(
        TFileHandle& file,
        i64 offset,
        TArrayRef<char> buffer,
        TFileIOCompletion* completion) override
    {
        Y_UNUSED(file);
        Y_UNUSED(offset);
        Y_UNUSED(buffer);
        Y_UNUSED(completion);
    }

    void AsyncReadV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<char>>& buffers,
        TFileIOCompletion* completion) override
    {
        Y_UNUSED(file);
        Y_UNUSED(offset);
        Y_UNUSED(buffers);
        Y_UNUSED(completion);
    }

    void AsyncWrite(
        TFileHandle& file,
        i64 offset,
        TArrayRef<const char> buffer,
        TFileIOCompletion* completion) override
    {
        Y_UNUSED(file);
        Y_UNUSED(offset);
        Y_UNUSED(buffer);
        Y_UNUSED(completion);
    }

    void AsyncWriteV(
        TFileHandle& file,
        i64 offset,
        const TVector<TArrayRef<const char>>& buffers,
        TFileIOCompletion* completion) override
    {
        Y_UNUSED(file);
        Y_UNUSED(offset);
        Y_UNUSED(buffers);
        Y_UNUSED(completion);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestProvider
    : IFileIOServiceProvider
{
    int Started = 0;
    int Stopped = 0;

    TVector<std::shared_ptr<TTestFileIOService>> FileIOs;

    void Start() override
    {
        ++Started;
    }

    void Stop() override
    {
        ++Stopped;

        for (auto& s: FileIOs) {
            s->Stop();
        }
    }

    IFileIOServicePtr CreateFileIOService(TStringBuf filePath) override
    {
        Y_UNUSED(filePath);

        auto service = std::make_shared<TTestFileIOService>();

        FileIOs.push_back(service);

        return service;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFileIOServiceProviderTest)
{
    Y_UNIT_TEST(ShouldCreateSingleFileIOService)
    {
        auto fileIO = std::make_shared<TTestFileIOService>();
        UNIT_ASSERT_VALUES_EQUAL(0, fileIO->Started);

        auto provider = CreateSingleFileIOServiceProvider(fileIO);
        provider->Start();

        UNIT_ASSERT_VALUES_EQUAL(1, fileIO->Started);

        auto io1 = provider->CreateFileIOService("foo");
        UNIT_ASSERT(io1 == fileIO);
        UNIT_ASSERT_VALUES_EQUAL(1, fileIO->Started);

        auto io2 = provider->CreateFileIOService("bar");
        UNIT_ASSERT(io2 == fileIO);
        auto io3 = provider->CreateFileIOService("foo");
        UNIT_ASSERT(io3 == fileIO);
        auto io4 = provider->CreateFileIOService("buz");
        UNIT_ASSERT(io4 == fileIO);

        UNIT_ASSERT_VALUES_EQUAL(1, fileIO->Started);
        UNIT_ASSERT_VALUES_EQUAL(0, fileIO->Stopped);

        provider->Stop();
        UNIT_ASSERT_VALUES_EQUAL(1, fileIO->Stopped);
    }

    void ShouldCreateOneServicePerOnePathImpl(ui32 pathsToServices)
    {
        UNIT_ASSERT(pathsToServices <= 1);

        TTestProvider upstream;

        auto provider = CreateFileIOServiceProvider(
            pathsToServices,
            [&] { return upstream.CreateFileIOService({}); });

        provider->Start();

        UNIT_ASSERT_VALUES_EQUAL(0, upstream.FileIOs.size());

        auto io1 = provider->CreateFileIOService("p1");
        UNIT_ASSERT_VALUES_EQUAL(1, upstream.FileIOs.size());
        UNIT_ASSERT(io1 == upstream.FileIOs[0]);
        UNIT_ASSERT_VALUES_EQUAL(1, upstream.FileIOs[0]->Started);

        auto io2 = provider->CreateFileIOService("p2");
        UNIT_ASSERT_VALUES_EQUAL(2, upstream.FileIOs.size());
        UNIT_ASSERT_VALUES_EQUAL(1, upstream.FileIOs[0]->Started);
        UNIT_ASSERT_VALUES_EQUAL(1, upstream.FileIOs[1]->Started);
        UNIT_ASSERT(io2 == upstream.FileIOs[1]);
        UNIT_ASSERT(io1 != io2);

        auto io3 = provider->CreateFileIOService("p3");
        UNIT_ASSERT_VALUES_EQUAL(3, upstream.FileIOs.size());
        UNIT_ASSERT(io2 != io3);

        auto io4 = provider->CreateFileIOService("p4");
        UNIT_ASSERT_VALUES_EQUAL(4, upstream.FileIOs.size());
        UNIT_ASSERT(io3 != io4);

        auto io5 = provider->CreateFileIOService("p1");
        UNIT_ASSERT_VALUES_EQUAL(4, upstream.FileIOs.size());
        UNIT_ASSERT(io1 == io5);

        auto io6 = provider->CreateFileIOService("p4");
        UNIT_ASSERT_VALUES_EQUAL(4, upstream.FileIOs.size());
        UNIT_ASSERT(io4 == io6);

        provider->Stop();
        for (const auto& s: upstream.FileIOs) {
            UNIT_ASSERT_VALUES_EQUAL(1, s->Started);
            UNIT_ASSERT_VALUES_EQUAL(1, s->Stopped);
        }
    }

    Y_UNIT_TEST(ShouldCreateOneServicePerOnePath)
    {
        ShouldCreateOneServicePerOnePathImpl(0);
        ShouldCreateOneServicePerOnePathImpl(1);
    }

    Y_UNIT_TEST(ShouldCreateOneServicePerTwoPaths)
    {
        const ui32 pathsToServices = 2;

        TTestProvider upstream;

        auto provider = CreateFileIOServiceProvider(
            pathsToServices,
            [&] { return upstream.CreateFileIOService({}); });

        provider->Start();

        UNIT_ASSERT_VALUES_EQUAL(0, upstream.FileIOs.size());

        auto io1 = provider->CreateFileIOService("p1");
        UNIT_ASSERT_VALUES_EQUAL(1, upstream.FileIOs.size());
        UNIT_ASSERT(io1);

        auto io2 = provider->CreateFileIOService("p2");
        UNIT_ASSERT_VALUES_EQUAL(1, upstream.FileIOs.size());
        UNIT_ASSERT(io1 == io2);

        auto io3 = provider->CreateFileIOService("p3");
        UNIT_ASSERT_VALUES_EQUAL(2, upstream.FileIOs.size());

        auto io4 = provider->CreateFileIOService("p4");
        UNIT_ASSERT_VALUES_EQUAL(2, upstream.FileIOs.size());

        UNIT_ASSERT(io3 == io4);
        UNIT_ASSERT(io1 != io4);

        auto io5 = provider->CreateFileIOService("p1");
        UNIT_ASSERT_VALUES_EQUAL(2, upstream.FileIOs.size());
        UNIT_ASSERT(io1 == io5);

        auto io6 = provider->CreateFileIOService("p6");
        UNIT_ASSERT_VALUES_EQUAL(3, upstream.FileIOs.size());
        UNIT_ASSERT(io1 != io6);
        UNIT_ASSERT(io3 != io6);

        auto io7 = provider->CreateFileIOService("p6");
        UNIT_ASSERT_VALUES_EQUAL(3, upstream.FileIOs.size());
        UNIT_ASSERT(io6 == io7);

        provider->Stop();
        for (const auto& s: upstream.FileIOs) {
            UNIT_ASSERT_VALUES_EQUAL(1, s->Started);
            UNIT_ASSERT_VALUES_EQUAL(1, s->Stopped);
        }
    }

    Y_UNIT_TEST(ShouldCreateOneServicePerFourPaths)
    {
        const ui32 pathsToServices = 4;
        const ui32 devicesPerPath = 64;
        const ui32 pathCount = 6;
        const ui32 expectedServiceCount = 2;

        TTestProvider upstream;

        auto provider = CreateFileIOServiceProvider(
            pathsToServices,
            [&] { return upstream.CreateFileIOService({}); });

        provider->Start();

        for (ui32 i = 0; i != pathCount; ++i) {
            const TString path =
                "/dev/disk/by-partlabel/NVMENBS0" + ToString(i);

            for (ui32 j = 0; j != devicesPerPath; ++j) {
                auto fileIO = provider->CreateFileIOService(path);
                UNIT_ASSERT(fileIO == upstream.FileIOs.back());
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(expectedServiceCount, upstream.FileIOs.size());

        for (const auto& s: upstream.FileIOs) {
            UNIT_ASSERT_VALUES_EQUAL(1, s->Started);
            UNIT_ASSERT_VALUES_EQUAL(0, s->Stopped);
        }

        provider->Stop();

        for (const auto& s: upstream.FileIOs) {
            UNIT_ASSERT_VALUES_EQUAL(1, s->Started);
            UNIT_ASSERT_VALUES_EQUAL(1, s->Stopped);
        }
    }
}

}   // namespace NCloud::NBlockStore::NServer
