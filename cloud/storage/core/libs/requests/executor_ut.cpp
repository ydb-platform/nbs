#include "executor.h"

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/gmock_in_unittest/gmock.h>

using ::testing::_;
using ::testing::AtLeast;
using ::testing::DoAll;
using ::testing::Expectation;
using ::testing::Return;
using ::testing::SetArgPointee;

namespace NCloud::NStorage::NRequests {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TRequestHandlerMock
    : public TRequestHandlerBase
{
    MOCK_METHOD(void, Process, (bool));
    MOCK_METHOD(void, Cancel, ());
};

// WARNING: created only to be used with TExecutor for testing purposes
struct TCompletionQueueMock
{
    MOCK_METHOD(void, Shutdown, ());
    MOCK_METHOD(bool, Next, (void** tag, bool* ok));
};

struct RequestsInFlightPlaceHolder
{};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TExecutorTest)
{
    Y_UNIT_TEST(MustStopCallingHandlerOnShutdown)
    {
        ILoggingServicePtr logging = CreateLoggingService("console");
        auto Log = logging->CreateLog("NFS_SERVER");

        TExecutor<TCompletionQueueMock, RequestsInFlightPlaceHolder> executor(
            "executor test",
            std::make_unique<TCompletionQueueMock>(),
            Log);

        auto& cq = *executor.CompletionQueue.get();

        TRequestHandlerMock handler;

        std::condition_variable processCV;
        std::mutex processMutex;
        bool isProcessCalled = false;
        EXPECT_CALL(handler, Process(_)).WillRepeatedly([&] {
            {
                std::lock_guard lock(processMutex);
                isProcessCalled = true;
            }
            processCV.notify_one();
        });

        void* handlerPointer = static_cast<void*>(&handler);
        EXPECT_CALL(cq, Next(_, _))
            .WillRepeatedly(
                DoAll(SetArgPointee<0>(handlerPointer), Return(true)));

        executor.Start();
        {
            std::unique_lock lk(processMutex);
            processCV.wait(lk, [&isProcessCalled] { return isProcessCalled; });
        }

        EXPECT_CALL(cq, Shutdown());

        EXPECT_CALL(cq, Next(_, _))
            .WillOnce(Return(false));

        executor.Shutdown();
    }
}

}   // namespace NCloud::NStorage::NRequests
