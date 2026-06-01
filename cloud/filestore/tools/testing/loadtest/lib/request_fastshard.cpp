#include "request.h"

#include <cloud/filestore/libs/storage/fastshard/client/client.h>
#include <cloud/filestore/libs/storage/fastshard/server/protos/fastshard.pb.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/public/api/protos/node.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <silk/fibers/fiber.h>
#include <silk/util/init.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/guid.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/system/mutex.h>

#include <mutex>

namespace NCloud::NFileStore::NLoadTest {

using namespace NThreading;
using namespace NCloud::NFileStore::NStorage::NFastShard;
using namespace NCloud::NFileStore::NStorage::NFastShard::NProtoSrv;
using silk::FiberScheduler;

namespace {

////////////////////////////////////////////////////////////////////////////////

static std::once_flag SilkOnceFlag;

static void EnsureSilkInitialized()
{
    std::call_once(SilkOnceFlag, [] {
        silk::initialize();
        FiberScheduler::initialize();
    });
}

////////////////////////////////////////////////////////////////////////////////

struct TFileState
{
    ui64 Handle = 0;
    ui64 Size = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Heap-allocated state for a single request fiber. A raw pointer to this is
// passed through the silk FIBER_PARAMETERS_SIZE-limited params slot.
struct TFiberState
{
    ui16 Port;
    TString ShardFileSystemId;
    NProto::EAction Action;
    ui64 ReadBytes;
    ui64 WriteBytes;
    ui64 InitialFileSize;
    TInstant Started;
    TPromise<TCompletedRequest> Promise;
    // Handle == 0 signals "no file yet; create one inside the fiber".
    TFileState File;
    std::weak_ptr<class TFastShardRequestGenerator> Self;
};

struct TFiberPtr
{
    TFiberState* Ptr;
};
static_assert(sizeof(TFiberPtr) <= silk::FIBER_PARAMETERS_SIZE);

////////////////////////////////////////////////////////////////////////////////

class TFastShardRequestGenerator final
    : public IRequestGenerator
    , public std::enable_shared_from_this<TFastShardRequestGenerator>
{
private:
    static constexpr ui32 DefaultIoSize = 4096;

    const NProto::TFastShardLoadSpec Spec;
    const TString ShardFileSystemId;
    const ui16 Port;
    const ui64 ReadBytes;
    const ui64 WriteBytes;
    const ui64 InitialFileSize;

    TVector<std::pair<ui64, NProto::EAction>> Actions;
    ui64 TotalRate = 0;

    TMutex StateLock;
    TVector<TFileState> Files;

public:
    TFastShardRequestGenerator(
            NProto::TFastShardLoadSpec spec,
            ILoggingServicePtr /*logging*/)
        : Spec(std::move(spec))
        , ShardFileSystemId(Spec.GetShardFileSystemId())
        , Port(static_cast<ui16>(Spec.GetFastShardPort()))
        , ReadBytes(Spec.GetReadBytes() ? Spec.GetReadBytes() : DefaultIoSize)
        , WriteBytes(Spec.GetWriteBytes() ? Spec.GetWriteBytes() : DefaultIoSize)
        , InitialFileSize(Spec.GetInitialFileSize())
    {
        Y_ENSURE(!ShardFileSystemId.empty(), "ShardFileSystemId must be set");
        Y_ENSURE(Port != 0, "FastShardPort must be set");

        for (const auto& a : Spec.GetActions()) {
            Y_ENSURE(a.GetRate() > 0, "action rate must be positive");
            TotalRate += a.GetRate();
            Actions.emplace_back(TotalRate, a.GetAction());
        }
        Y_ENSURE(!Actions.empty(), "at least one action required");

        EnsureSilkInitialized();
    }

    bool HasNextRequest() override
    {
        return true;
    }

    TFuture<TCompletedRequest> ExecuteNextRequest() override
    {
        auto action = PeekNextAction();
        switch (action) {
            case NProto::ACTION_READ:  return DoRead();
            case NProto::ACTION_WRITE: return DoWrite();
            default:
                Y_ABORT("unexpected action: %u", static_cast<ui32>(action));
        }
    }

    void ReturnFile(TFileState file)
    {
        TGuard<TMutex> g(StateLock);
        Files.emplace_back(std::move(file));
    }

private:
    NProto::EAction PeekNextAction()
    {
        const auto r = RandomNumber(TotalRate);
        auto it = std::lower_bound(
            Actions.begin(),
            Actions.end(),
            r + 1,
            [](const std::pair<ui64, NProto::EAction>& p, ui64 v) {
                return p.first < v;
            });
        Y_ABORT_UNLESS(it != Actions.end());
        return it->second;
    }

    TFileState PopFile(TVector<TFileState>::iterator it)
    {
        std::swap(*it, Files.back());
        auto f = std::move(Files.back());
        Files.pop_back();
        return f;
    }

    TFuture<TCompletedRequest> SpawnFiber(std::unique_ptr<TFiberState> state)
    {
        auto future = state->Promise.GetFuture();
        auto* raw = state.release();

        const int r = FiberScheduler::run(
            +[](TFiberPtr* p) noexcept -> int {
                std::unique_ptr<TFiberState> s(p->Ptr);
                RunFiber(std::move(s));
                return 0;
            },
            TFiberPtr{raw},
            nullptr);
        Y_ABORT_UNLESS(r == 0, "FiberScheduler::run failed: %s", ::strerror(r));

        return future;
    }

    static void RunFiber(std::unique_ptr<TFiberState> s) noexcept
    {
        TClient client(s->Port);
        auto ptr = s->Self.lock();

        // Create a file if the pool had nothing to offer.
        if (s->File.Handle == 0) {
            if (!ptr) {
                s->Promise.SetValue({s->Action, s->Started,
                    MakeError(E_FAIL, "cancelled")});
                return;
            }

            TRequest req;
            req.SetFileSystemId(s->ShardFileSystemId);
            auto* body = req.MutableCreateHandle();
            body->SetNodeId(RootNodeId);
            body->SetName(CreateGuidAsString());
            body->SetFlags(NProto::TCreateHandleRequest::E_CREATE);
            body->SetMode(0664);

            auto resp = client.Send(req);

            const auto& createErr = resp.GetError().GetCode() != 0
                ? resp.GetError()
                : resp.GetCreateHandle().GetError();
            if (createErr.GetCode() != 0) {
                s->Promise.SetValue({s->Action, s->Started, createErr});
                return;
            }

            s->File.Handle = resp.GetCreateHandle().GetHandle();
            s->File.Size = 0;

            if (s->InitialFileSize > 0) {
                TRequest wReq;
                wReq.SetFileSystemId(s->ShardFileSystemId);
                auto* wb = wReq.MutableWriteData();
                wb->SetHandle(s->File.Handle);
                wb->SetOffset(0);
                wb->SetBuffer(TString(s->InitialFileSize, '\0'));
                client.Send(wReq);
                s->File.Size = s->InitialFileSize;
            }
        }

        // Build and execute the actual read or write request.
        TRequest req;
        req.SetFileSystemId(s->ShardFileSystemId);

        if (s->Action == NProto::ACTION_READ) {
            const ui64 slotCount = s->File.Size / s->ReadBytes;
            const ui64 offset = RandomNumber(slotCount) * s->ReadBytes;
            auto* body = req.MutableReadData();
            body->SetHandle(s->File.Handle);
            body->SetOffset(offset);
            body->SetLength(s->ReadBytes);
        } else {
            const ui64 offset = s->File.Size;
            s->File.Size += s->WriteBytes;
            auto* body = req.MutableWriteData();
            body->SetHandle(s->File.Handle);
            body->SetOffset(offset);
            body->SetBuffer(TString(s->WriteBytes, '\0'));
        }

        auto resp = client.Send(req);

        NProto::TError err;
        if (resp.GetError().GetCode() != 0) {
            err = resp.GetError();
        } else if (resp.HasReadData()) {
            err = resp.GetReadData().GetError();
        } else if (resp.HasWriteData()) {
            err = resp.GetWriteData().GetError();
        }

        if (ptr) {
            ptr->ReturnFile(std::move(s->File));
        }

        s->Promise.SetValue({s->Action, s->Started, std::move(err)});
    }

    TFuture<TCompletedRequest> DoRead()
    {
        TFileState file;
        {
            TGuard<TMutex> g(StateLock);
            for (auto it = Files.begin(); it != Files.end(); ++it) {
                if (it->Size >= ReadBytes) {
                    file = PopFile(it);
                    break;
                }
            }
        }

        auto state = std::make_unique<TFiberState>();
        state->Port = Port;
        state->ShardFileSystemId = ShardFileSystemId;
        state->Action = NProto::ACTION_READ;
        state->ReadBytes = ReadBytes;
        state->WriteBytes = WriteBytes;
        state->InitialFileSize = InitialFileSize;
        state->Started = TInstant::Now();
        state->Promise = NewPromise<TCompletedRequest>();
        state->Self = weak_from_this();
        state->File = std::move(file);

        return SpawnFiber(std::move(state));
    }

    TFuture<TCompletedRequest> DoWrite()
    {
        TFileState file;
        {
            TGuard<TMutex> g(StateLock);
            if (!Files.empty()) {
                file = PopFile(
                    Files.begin() + RandomNumber(Files.size()));
            }
        }

        auto state = std::make_unique<TFiberState>();
        state->Port = Port;
        state->ShardFileSystemId = ShardFileSystemId;
        state->Action = NProto::ACTION_WRITE;
        state->ReadBytes = ReadBytes;
        state->WriteBytes = WriteBytes;
        state->InitialFileSize = InitialFileSize;
        state->Started = TInstant::Now();
        state->Promise = NewPromise<TCompletedRequest>();
        state->Self = weak_from_this();
        state->File = std::move(file);

        return SpawnFiber(std::move(state));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRequestGeneratorPtr CreateFastShardRequestGenerator(
    NProto::TFastShardLoadSpec spec,
    ILoggingServicePtr logging)
{
    return std::make_shared<TFastShardRequestGenerator>(
        std::move(spec),
        std::move(logging));
}

}   // namespace NCloud::NFileStore::NLoadTest
