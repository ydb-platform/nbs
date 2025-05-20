#include "executor.h"

#include "context.h"

namespace NCloud::NFileStore::NLoadTest {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TExecutor::TExecutor(const TAppContext& ctx, TActionList actions)
    : Context(ctx)
    , Actions(std::move(actions))
{
    ThreadPool.Start();
}

void TExecutor::Run()
{
    while (!AtomicGet(Context.ShouldStop)) {
        ui32 current = -1;

        with_lock (Lock) {
            if (DoneCount == Actions.size()) {
                break;
            }

            current = DoneCount++;
        }

        RunAction(current).Subscribe([=, this] (auto future) {
            future.GetValue();  // re-throw exception
            ReadyEvent.Signal();
        });

        ReadyEvent.WaitI();
        ReadyEvent.Reset();
    }
}

TFuture<void> TExecutor::RunAction(ui32 action)
{
    auto promise = NewPromise();
    auto future = promise.GetFuture();

    ThreadPool.SafeAddFunc(
        [action = std::move(Actions[action]), promise = std::move(promise)] () mutable {
            try {
                action();
                promise.SetValue();
            } catch (...) {
                Y_ABORT("should not happen: %s", CurrentExceptionMessage().c_str());
            }
        }
    );

    return future;
}

}   // namespace NCloud::NFileStore::NLoadTest
