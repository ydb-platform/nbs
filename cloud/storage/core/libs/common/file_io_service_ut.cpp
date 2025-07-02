#include "file_io_service.h"

#include <library/cpp/testing/gmock_in_unittest/gmock.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/system/file.h>

#include <latch>
#include <thread>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestFileIOService final
    : public IFileIOService
{
    MOCK_METHOD(void, Start, (), (final));
    MOCK_METHOD(void, Stop, (), (final));

    MOCK_METHOD(
        void,
        AsyncRead,
        (TFileHandle&, i64, TArrayRef<char>, TFileIOCompletion*),
        (final));

    MOCK_METHOD(
        void,
        AsyncReadV,
        (TFileHandle&,
         i64,
         const TVector<TArrayRef<char>>&,
         TFileIOCompletion*),
        (final));

    MOCK_METHOD(
        void,
        AsyncWrite,
        (TFileHandle&, i64, TArrayRef<const char>, TFileIOCompletion*),
        (final));

    MOCK_METHOD(
        void,
        AsyncWriteV,
        (TFileHandle&,
         i64,
         const TVector<TArrayRef<const char>>&,
         TFileIOCompletion*),
        (final));
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFileIOServiceTest)
{
    Y_UNIT_TEST(ShouldStubServiceCB)
    {
        auto service = CreateFileIOServiceStub();
        service->Start();

        TFileHandle dummy {INVALID_FHANDLE};

        TArrayRef<char> buffer { nullptr, 1024 };

        {
            NProto::TError error = MakeError(E_FAIL);

            service->AsyncRead(dummy, 0, buffer, [&] (const auto& er, ui32 n) {
                UNIT_ASSERT_VALUES_EQUAL(buffer.size(), n);
                error = er;
            });

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        }

        {
            NProto::TError error = MakeError(E_FAIL);

            service->AsyncWrite(dummy, 0, buffer, [&] (const auto& er, ui32 n) {
                UNIT_ASSERT_VALUES_EQUAL(buffer.size(), n);
                error = er;
            });

            UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        }

        service->Stop();
    }

    Y_UNIT_TEST(ShouldStubServiceFuture)
    {
        auto service = CreateFileIOServiceStub();
        service->Start();

        TFileHandle dummy {INVALID_FHANDLE};

        TArrayRef<char> buffer {nullptr, 1024};

        UNIT_ASSERT_VALUES_EQUAL(
            buffer.size(),
            service->AsyncRead(dummy, 0, buffer).GetValueSync());

        UNIT_ASSERT_VALUES_EQUAL(
            buffer.size(),
            service->AsyncWrite(dummy, 0, buffer).GetValueSync());

        service->Stop();
    }

    Y_UNIT_TEST(ShouldRoundRobin)
    {
        using namespace ::testing;

        const size_t RequestsPerService = 3;

        TVector fileIOs{
            std::make_shared<TTestFileIOService>(),
            std::make_shared<TTestFileIOService>(),
            std::make_shared<TTestFileIOService>()};

        auto service = CreateRoundRobinFileIOService(
            TVector<IFileIOServicePtr>{fileIOs[0], fileIOs[1], fileIOs[2]});

        for (auto& fileIO: fileIOs) {
            EXPECT_CALL(*fileIO, Start()).WillOnce(Return());

            EXPECT_CALL(*fileIO, AsyncRead(_, 0, _, _))
                .Times(RequestsPerService)
                .WillRepeatedly(Return());
            EXPECT_CALL(*fileIO, AsyncReadV(_, 0, _, _))
                .Times(RequestsPerService)
                .WillRepeatedly(Return());
            EXPECT_CALL(*fileIO, AsyncWrite(_, 0, _, _))
                .Times(RequestsPerService)
                .WillRepeatedly(Return());
            EXPECT_CALL(*fileIO, AsyncWriteV(_, 0, _, _))
                .Times(RequestsPerService)
                .WillRepeatedly(Return());

            EXPECT_CALL(*fileIO, Stop()).WillOnce(Return());
        }

        service->Start();

        TFileHandle dummy {INVALID_FHANDLE};
        TArrayRef<char> buffer {nullptr, 1024};

        for (size_t i = 0; i != RequestsPerService * fileIOs.size(); ++i) {
            service->AsyncRead(dummy, 0, buffer, [](auto...) {});
            service->AsyncWrite(dummy, 0, buffer, [](auto...) {});

            service->AsyncReadV(
                dummy,
                0,
                TVector<TArrayRef<char>>{buffer},
                [](auto...) {});
            service->AsyncWriteV(
                dummy,
                0,
                TVector<TArrayRef<const char>>{buffer},
                [](auto...) {});
        }

        service->Stop();
    }

    Y_UNIT_TEST(ShouldProcessConcurrentRequests)
    {
        using namespace ::testing;

        const ui32 requestsPerClient = 1024;
        const ui32 clientCount = 8;
        const ui32 totalRequestCount = requestsPerClient * clientCount;

        auto fileIO = std::make_shared<TTestFileIOService>();

        EXPECT_CALL(*fileIO, Start()).WillOnce(Return());
        EXPECT_CALL(*fileIO, Stop()).WillOnce(Return());

        // totalRequestCount requests of each type
        std::latch done{4 * totalRequestCount};

        auto onRequest = [&done] (auto...) {
            done.count_down();
        };

        EXPECT_CALL(*fileIO, AsyncRead(_, 0, _, _))
            .Times(totalRequestCount)
            .WillRepeatedly(onRequest);
        EXPECT_CALL(*fileIO, AsyncReadV(_, 0, _, _))
            .Times(totalRequestCount)
            .WillRepeatedly(onRequest);
        EXPECT_CALL(*fileIO, AsyncWrite(_, 0, _, _))
            .Times(totalRequestCount)
            .WillRepeatedly(onRequest);
        EXPECT_CALL(*fileIO, AsyncWriteV(_, 0, _, _))
            .Times(totalRequestCount)
            .WillRepeatedly(onRequest);

        auto service = CreateConcurrentFileIOService("SQ", fileIO);
        service->Start();

        TVector<std::thread> clients;
        clients.reserve(clientCount);

        TFileIOCompletion completion{
            .Func = [] (auto...){}
        };

        TFileHandle dummy{INVALID_FHANDLE};
        TArrayRef<char> buffer{nullptr, 1024};

        std::latch sync{clientCount};

        for (ui32 i = 0; i != clientCount; ++i) {
            clients.emplace_back(
                [&]
                {
                    sync.arrive_and_wait();

                    for (ui32 i = 0; i != requestsPerClient; ++i) {
                        service->AsyncRead(dummy, 0, buffer, &completion);
                        service->AsyncWrite(dummy, 0, buffer, &completion);

                        service->AsyncReadV(
                            dummy,
                            0,
                            TVector<TArrayRef<char>>{buffer},
                            &completion);
                        service->AsyncWriteV(
                            dummy,
                            0,
                            TVector<TArrayRef<const char>>{buffer},
                            &completion);
                    }
                });
        }

        for (auto& client: clients) {
            client.join();
        }

        done.wait();

        service->Stop();
    }
}

}   // namespace NCloud
