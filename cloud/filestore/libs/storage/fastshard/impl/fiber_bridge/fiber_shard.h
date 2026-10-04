#pragma once

#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/util/logger.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/deque.h>
#include <util/string/builder.h>
#include <util/system/spinlock.h>

#include <cstring>
#include <memory>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

//
// The GNU strerror_r variant - the build defines _GNU_SOURCE. Thread-safe,
// unlike strerror.
//

inline TString FiberSpawnErrorText(int err)
{
    char buf[64] = {};
    return ::strerror_r(err, buf, sizeof(buf));
}

/**
 * Tracks the termination futures of the fibers spawned by TFiberShard so
 * that no spawned fiber can outlive the shard object. The caller-visible
 * promise of each call is fulfilled from inside the still-running fiber
 * (before the thread-mode exit hop), so it cannot serve as a join point -
 * only the termination future, which the fiber runtime sets after the
 * fiber has left every scheduler structure, can.
 */
class TInflightFiberRegistry
{
private:
    TAdaptiveLock Lock;
    TDeque<std::unique_ptr<silk::FiberFuture>> Futures;

public:
    /**
     * Returns a fresh termination future to pass to FiberScheduler::run.
     * Prunes the futures of already-finished fibers, so the deque stays
     * bounded by the number of concurrently running fibers.
     *
     * @return - Pointer owned by the registry, stable until WaitAll.
     */
    silk::FiberFuture* Acquire()
    {
        with_lock (Lock) {
            for (auto it = Futures.begin(); it != Futures.end();) {
                int fiberResult = 0;
                if ((*it)->isSet(&fiberResult)) {
                    it = Futures.erase(it);
                } else {
                    ++it;
                }
            }

            Futures.push_back(std::make_unique<silk::FiberFuture>());
            return Futures.back().get();
        }
    }

    /**
     * Removes a future whose fiber failed to spawn - the runtime will
     * never set it, so WaitAll must not wait for it.
     *
     * @param future - Pointer previously returned by Acquire.
     */
    void Remove(silk::FiberFuture* future)
    {
        with_lock (Lock) {
            for (auto it = Futures.begin(); it != Futures.end(); ++it) {
                if (it->get() == future) {
                    Futures.erase(it);
                    return;
                }
            }
        }
    }

    /**
     * Waits until every tracked fiber has terminated. Once this returns,
     * no tracked fiber can touch the shard, the scheduler lists or its
     * own parameters anymore.
     */
    void WaitAll()
    {
        TDeque<std::unique_ptr<silk::FiberFuture>> local;
        with_lock (Lock) {
            std::swap(local, Futures);
        }

        for (const auto& future: local) {
            const int fiberResult = future->wait();
            if (fiberResult) {
                SILK_WARN(
                    "shard fiber exited with error: %s",
                    FiberSpawnErrorText(fiberResult).c_str());
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TFiberShardImpl>
class TFiberShard: public IFileSystemShard
{
private:
    std::shared_ptr<TFiberShardImpl> Impl;

    //
    // Mutable because the const CollectStats also spawns a fiber.
    //

    mutable TInflightFiberRegistry InflightFibers;

public:
    explicit TFiberShard(std::shared_ptr<TFiberShardImpl> impl)
        : Impl(std::move(impl))
    {}

    /**
     * Joins every fiber this shard has spawned, including the detached
     * TearDown fiber. Without the join a fiber tail could outlive the
     * shard and, in tests, the FiberScheduler itself - the environment
     * teardown then catches the fiber in the scheduler lists and aborts.
     */
    ~TFiberShard() override
    {
        InflightFibers.WaitAll();
    }

private:
#define FAST_SHARD_FB_DEFINE_METHOD(name, ns, ...)                             \
    struct TFiberShard##name##Params                                           \
    {                                                                          \
        std::shared_ptr<TFiberShardImpl> Impl;                                 \
        std::shared_ptr<ns::T##name##Request> Request;                         \
        NThreading::TPromise<ns::T##name##Response> Promise;                   \
    };                                                                         \
                                                                               \
    static int name##FiberMain(TFiberShard##name##Params* params) noexcept     \
    {                                                                          \
        auto response = params->Impl->name(std::move(*params->Request));       \
                                                                               \
        /*                                                                     \
         * SetValue synchronously runs the subscribed callbacks. Thread mode   \
         * moves this fiber to the worker pool for the duration of the call,   \
         * so a slow or blocking callback cannot stall the fibers homed on     \
         * this scheduler thread. The callbacks still run on the fiber's       \
         * 64KiB stack.                                                        \
         */                                                                    \
                                                                               \
        silk::FiberScheduler::ThreadModeScope threadModeScope;                 \
        params->Promise.SetValue(std::move(response));                         \
        return 0;                                                              \
    }                                                                          \
    // FAST_SHARD_FB_DEFINE_METHOD

    FAST_SHARD_PRIVATE_METHODS(FAST_SHARD_FB_DEFINE_METHOD, NProtoPrivate)
    FAST_SHARD_PUBLIC_METHODS(FAST_SHARD_FB_DEFINE_METHOD, NProto)

#undef FAST_SHARD_FB_DEFINE_METHOD

    struct TFiberShardCollectStatsParams
    {
        std::shared_ptr<TFiberShardImpl> Impl;
        TFileSystemShardStats* Stats;
        NThreading::TPromise<NProto::TError> Promise;
    };

    static int CollectStatsFiberMain(TFiberShardCollectStatsParams* params)
        noexcept
    {
        auto e = params->Impl->CollectStats(params->Stats);

        //
        // See the comment about SetValue in the request method fiber main.
        //

        silk::FiberScheduler::ThreadModeScope threadModeScope;
        params->Promise.SetValue(std::move(e));
        return 0;
    }

    struct TFiberShardInitParams
    {
        std::shared_ptr<TFiberShardImpl> Impl;
        NThreading::TPromise<NProto::TError> Promise;
    };

    static int InitFiberMain(TFiberShardInitParams* params) noexcept
    {
        auto e = params->Impl->Init();

        //
        // See the comment about SetValue in the request method fiber main.
        //

        silk::FiberScheduler::ThreadModeScope threadModeScope;
        params->Promise.SetValue(std::move(e));
        return 0;
    }

    struct TFiberShardFormatParams
    {
        std::shared_ptr<TFiberShardImpl> Impl;
        NThreading::TPromise<NProto::TError> Promise;
    };

    static int FormatFiberMain(TFiberShardFormatParams* params) noexcept
    {
        auto e = params->Impl->Format();

        //
        // See the comment about SetValue in the request method fiber main.
        //

        silk::FiberScheduler::ThreadModeScope threadModeScope;
        params->Promise.SetValue(e);
        return 0;
    }

    struct TFiberShardTearDownParams
    {
        std::shared_ptr<TFiberShardImpl> Impl;
    };

    static int TearDownFiberMain(TFiberShardTearDownParams* params) noexcept
    {
        params->Impl->TearDown();
        return 0;
    }

public:
    [[nodiscard]] NThreading::TFuture<NProto::TError> Init() override
    {
        auto promise = NThreading::NewPromise<NProto::TError>();
        auto future = promise.GetFuture();

        auto* fiberFuture = InflightFibers.Acquire();
        const int r = silk::FiberScheduler::run(
            InitFiberMain,
            TFiberShardInitParams{.Impl = Impl, .Promise = promise},
            fiberFuture);
        if (r) {
            InflightFibers.Remove(fiberFuture);
            promise.SetValue(MakeError(
                E_FAIL,
                TStringBuilder()
                    << "failed to spawn fiber: " << FiberSpawnErrorText(r)));
        }

        return future;
    }

    [[nodiscard]] NThreading::TFuture<NProto::TError> Format() override
    {
        auto promise = NThreading::NewPromise<NProto::TError>();
        auto future = promise.GetFuture();

        auto* fiberFuture = InflightFibers.Acquire();
        const int r = silk::FiberScheduler::run(
            FormatFiberMain,
            TFiberShardFormatParams{.Impl = Impl, .Promise = promise},
            fiberFuture);
        if (r) {
            InflightFibers.Remove(fiberFuture);
            promise.SetValue(MakeError(
                E_FAIL,
                TStringBuilder()
                    << "failed to spawn fiber: " << FiberSpawnErrorText(r)));
        }

        return future;
    }

    void TearDown() override
    {
        auto* fiberFuture = InflightFibers.Acquire();
        const int r = silk::FiberScheduler::run(
            TearDownFiberMain,
            TFiberShardTearDownParams{.Impl = Impl},
            fiberFuture);
        if (r) {
            InflightFibers.Remove(fiberFuture);
            SILK_ERROR(
                "failed to spawn tear-down fiber: %s",
                FiberSpawnErrorText(r).c_str());
        }
    }

#define FAST_SHARD_FB_DEFINE_METHOD(name, ns, ...)                             \
    NThreading::TFuture<ns::T##name##Response> name(                           \
        ns::T##name##Request request) override                                 \
    {                                                                          \
        auto promise = NThreading::NewPromise<ns::T##name##Response>();        \
        auto future = promise.GetFuture();                                     \
                                                                               \
        auto* fiberFuture = InflightFibers.Acquire();                          \
        int r = silk::FiberScheduler::run(                                     \
            name##FiberMain,                                                   \
            TFiberShard##name##Params{                                         \
                .Impl = Impl,                                                  \
                .Request = std::make_shared<ns::T##name##Request>(             \
                    std::move(request)),                                       \
                .Promise = promise,                                            \
            },                                                                 \
            fiberFuture);                                                      \
        if (r) {                                                               \
            InflightFibers.Remove(fiberFuture);                                \
            ns::T##name##Response response;                                    \
            *response.MutableError() = MakeError(                              \
                E_FAIL,                                                        \
                TStringBuilder()                                               \
                    << "failed to spawn fiber: " << FiberSpawnErrorText(r));   \
            promise.SetValue(std::move(response));                             \
        }                                                                      \
                                                                               \
        return future;                                                         \
    }                                                                          \
    // FAST_SHARD_FB_DEFINE_METHOD

    FAST_SHARD_PRIVATE_METHODS(FAST_SHARD_FB_DEFINE_METHOD, NProtoPrivate)
    FAST_SHARD_PUBLIC_METHODS(FAST_SHARD_FB_DEFINE_METHOD, NProto)

#undef FAST_SHARD_FB_DEFINE_METHOD

    [[nodiscard]] NThreading::TFuture<NProto::TError> CollectStats(
        TFileSystemShardStats* stats) const override
    {
        auto promise = NThreading::NewPromise<NProto::TError>();
        auto future = promise.GetFuture();

        auto* fiberFuture = InflightFibers.Acquire();
        int r = silk::FiberScheduler::run(
            CollectStatsFiberMain,
            TFiberShardCollectStatsParams{
                .Impl = Impl,
                .Stats = stats,
                .Promise = promise,
            },
            fiberFuture);
        if (r) {
            InflightFibers.Remove(fiberFuture);
            promise.SetValue(MakeError(
                E_FAIL,
                TStringBuilder()
                    << "failed to spawn fiber: " << FiberSpawnErrorText(r)));
        }

        return future;
    }

    //
    // The layout is immutable after initialization and its dump does no page
    // IO, so no fiber is needed here.
    //

    void DumpLayoutHtml(IOutputStream& out) const override
    {
        Impl->DumpLayoutHtml(out);
    }

    void DumpLayoutJson(IOutputStream& out) const override
    {
        Impl->DumpLayoutJson(out);
    }
};

}   // namespace NCloud::NFileStore::NStorage::NFastShard
