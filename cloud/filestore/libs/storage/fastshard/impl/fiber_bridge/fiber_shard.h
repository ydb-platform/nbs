#pragma once

#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/fiber.h>

#include <library/cpp/threading/future/future.h>

#include <util/string/builder.h>

#include <cstring>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////
// TODO(#6958):
// * strerror -> strerror_r (it's thread-safe)
// * deal with SetValue being called from fiber context (it triggers arbitrary
//  callback invocation)

template <typename TFiberShardImpl>
class TFiberShard: public IFileSystemShard
{
private:
    std::shared_ptr<TFiberShardImpl> Impl;

public:
    explicit TFiberShard(std::shared_ptr<TFiberShardImpl> impl)
        : Impl(std::move(impl))
    {}

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
        params->Promise.SetValue(std::move(e));
        return 0;
    }

public:
#define FAST_SHARD_FB_DEFINE_METHOD(name, ns, ...)                             \
    NThreading::TFuture<ns::T##name##Response> name(                           \
        ns::T##name##Request request) override                                 \
    {                                                                          \
        auto promise = NThreading::NewPromise<ns::T##name##Response>();        \
        auto future = promise.GetFuture();                                     \
                                                                               \
        int r = silk::FiberScheduler::run(                                     \
            name##FiberMain,                                                   \
            TFiberShard##name##Params{                                         \
                .Impl = Impl,                                                  \
                .Request = std::make_shared<ns::T##name##Request>(             \
                    std::move(request)),                                       \
                .Promise = promise,                                            \
            },                                                                 \
            nullptr /* future */);                                             \
        if (r) {                                                               \
            ns::T##name##Response response;                                    \
            *response.MutableError() = MakeError(                              \
                E_FAIL,                                                        \
                TStringBuilder()                                               \
                    << "failed to spawn fiber: " << ::strerror(r));            \
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

        int r = silk::FiberScheduler::run(
            CollectStatsFiberMain,
            TFiberShardCollectStatsParams{
                .Impl = Impl,
                .Stats = stats,
                .Promise = promise,
            },
            nullptr /* future */);
        if (r) {
            promise.SetValue(MakeError(
                E_FAIL,
                TStringBuilder()
                    << "failed to spawn fiber: " << ::strerror(r)));
        }

        return future;
    }

    //
    // The layout is immutable after construction and its dump does no
    // page IO, so no fiber is needed here.
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
