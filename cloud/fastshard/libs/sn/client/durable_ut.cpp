#include <cloud/fastshard/libs/common/context.h>
#include <cloud/fastshard/libs/sn/client/durable.h>
#include <cloud/fastshard/libs/sn/iface/storage_node.h>
#include <cloud/fastshard/libs/sn/testlib/fake_storage_node.h>
#include <cloud/fastshard/public/api/protos/device.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <util/datetime/base.h>
#include <util/system/guard.h>

#include <gtest/gtest.h>

using namespace NCloud::NFastShard;
using silk::FiberScheduler;

namespace {

////////////////////////////////////////////////////////////////////////////////

NCloud::NProto::TWriteLogRecordResponse Rejected()
{
    NCloud::NProto::TWriteLogRecordResponse response;
    *response.MutableError() = MakeError(NCloud::E_REJECTED);
    return response;
}

TDurableClientConfig FastConfig()
{
    return {
        .RetryTimeout = TDuration::Seconds(10),
        .MinRetryBackoff = TDuration::MicroSeconds(100),
        .MaxRetryBackoff = TDuration::MilliSeconds(1),
    };
}

struct TFixture
{
    std::shared_ptr<TFakeStorageNode> Storage =
        std::make_shared<TFakeStorageNode>();
    TDurableClientMetricsPtr Metrics =
        std::make_shared<TDurableClientMetrics>();
    IStorageNodePtr Client;
    TFastShardContext Ctx{0};

    explicit TFixture(TDurableClientConfig config = FastConfig())
        : Client(CreateDurableClient(Storage, config, Metrics))
    {}

    NCloud::NProto::TWriteLogRecordResponse Write(ui64 lsn)
    {
        NCloud::NProto::TWriteLogRecordRequest request;
        request.SetDeviceUUID("dev");
        request.SetLogSequenceNumber(lsn);
        return Client->WriteLogRecord(Ctx, std::move(request));
    }

    size_t WriteCount()
    {
        with_lock (Storage->Lock) {
            return Storage->WriteCalls.size();
        }
    }

    // Suspends the fiber until the fake has seen `count` writes.
    void WaitForWrites(size_t count)
    {
        while (WriteCount() < count) {
            FiberScheduler::sleep(TDuration::MilliSeconds(1).NanoSeconds());
        }
    }
};

struct TWriteParams
{
    TFixture* Fx;
    ui32* ErrorCode;
};

static_assert(sizeof(TWriteParams) <= silk::FIBER_PARAMETERS_SIZE);

int WriteFiber(TWriteParams* params) noexcept
{
    *params->ErrorCode = params->Fx->Write(1).GetError().GetCode();
    return 0;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(DurableClientTest, ShouldForwardSuccessfulResponse)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TFixture fx;

            EXPECT_EQ(0u, fx.Write(1).GetError().GetCode());
            EXPECT_EQ(1u, fx.WriteCount());
            EXPECT_EQ(0u, fx.Metrics->WriteLogRecordRetries.load());
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(DurableClientTest, ShouldRetryRetriableErrorUntilSuccess)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TFixture fx;
            fx.Storage->WriteRespQueue.push_back(Rejected());
            fx.Storage->WriteRespQueue.push_back(Rejected());

            EXPECT_EQ(0u, fx.Write(7).GetError().GetCode());
            EXPECT_EQ(3u, fx.WriteCount());
            EXPECT_EQ(2u, fx.Metrics->WriteLogRecordRetries.load());

            // every attempt carries the original request
            for (const auto& call: fx.Storage->WriteCalls) {
                EXPECT_EQ(7u, call.GetLogSequenceNumber());
            }
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(DurableClientTest, ShouldNotRetryNonRetriableError)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TFixture fx;
            *fx.Storage->WriteResp.MutableError() =
                MakeError(NCloud::E_ARGUMENT);

            EXPECT_EQ(
                static_cast<ui32>(NCloud::E_ARGUMENT),
                fx.Write(1).GetError().GetCode());
            EXPECT_EQ(1u, fx.WriteCount());
            EXPECT_EQ(0u, fx.Metrics->WriteLogRecordRetries.load());
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(DurableClientTest, ShouldGiveUpAfterRetryTimeout)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            auto config = FastConfig();
            config.RetryTimeout = TDuration::MilliSeconds(50);
            TFixture fx(config);
            fx.Storage->WriteResp = Rejected();

            EXPECT_EQ(
                static_cast<ui32>(NCloud::E_REJECTED),
                fx.Write(1).GetError().GetCode());
            EXPECT_GT(fx.WriteCount(), 1u);
            EXPECT_EQ(
                fx.WriteCount() - 1,
                fx.Metrics->WriteLogRecordRetries.load());
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(DurableClientTest, ShouldNotRetryWhenRetryTimeoutIsZero)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            auto config = FastConfig();
            config.RetryTimeout = TDuration::Zero();
            TFixture fx(config);
            fx.Storage->WriteResp = Rejected();

            EXPECT_EQ(
                static_cast<ui32>(NCloud::E_REJECTED),
                fx.Write(1).GetError().GetCode());
            EXPECT_EQ(1u, fx.WriteCount());
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(DurableClientTest, ShouldStopRetryingOnCancel)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TFixture fx;
            fx.Storage->WriteResp = Rejected();

            ui32 errorCode = 0;
            silk::FiberFuture done;
            const int r = FiberScheduler::run(
                WriteFiber,
                TWriteParams{.Fx = &fx, .ErrorCode = &errorCode},
                &done);
            Y_ABORT_UNLESS(r == 0);

            fx.WaitForWrites(3);
            fx.Ctx.Cancel.set();
            done.wait();

            EXPECT_EQ(static_cast<ui32>(NCloud::E_REJECTED), errorCode);
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(DurableClientTest, ShouldEndBackoffOnCancel)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            auto config = FastConfig();
            config.MinRetryBackoff = TDuration::Hours(1);
            config.MaxRetryBackoff = TDuration::Hours(1);
            TFixture fx(config);
            fx.Storage->WriteResp = Rejected();

            ui32 errorCode = 0;
            silk::FiberFuture done;
            const int r = FiberScheduler::run(
                WriteFiber,
                TWriteParams{.Fx = &fx, .ErrorCode = &errorCode},
                &done);
            Y_ABORT_UNLESS(r == 0);

            // the fiber is in its first hour-long backoff; the cancel ends it
            fx.WaitForWrites(1);
            fx.Ctx.Cancel.set();
            done.wait();

            EXPECT_EQ(static_cast<ui32>(NCloud::E_REJECTED), errorCode);
            EXPECT_EQ(1u, fx.WriteCount());
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}
