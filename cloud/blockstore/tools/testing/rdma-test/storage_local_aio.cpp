#include "storage.h"

#include "probes.h"

#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/thread.h>

#include <util/system/file.h>
#include <util/system/spin_wait.h>
#include <util/system/thread.h>

#include <libaio.h>

#define AIO_RING_MAGIC 0xA10A10A1

namespace NCloud::NBlockStore {

using namespace NThreading;

LWTRACE_USING(BLOCKSTORE_TEST_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 MAX_EVENTS_BATCH = 128;
constexpr ui32 WAIT_TIMEOUT = 10;   // us

////////////////////////////////////////////////////////////////////////////////

class TAsyncIOContext
{
#if defined(AIO_RING_MAGIC)
    struct io_ring
    {
        ui32 id;     // kernel internal index number
        ui32 nr;     // number of io_events
        ui32 head;   // reader position
        ui32 tail;   // writes position

        ui32 magic;   // AIO_RING_MAGIC
        ui32 compat_features;
        ui32 incompat_features;
        ui32 header_length;   // size of aio_ring

        io_event events[0];   // variable array of size nr
    };
#endif   // AIO_RING_MAGIC

private:
    io_context* Context = nullptr;

public:
    TAsyncIOContext(size_t nr)
    {
        int ret = io_setup(nr, &Context);
        Y_ABORT_UNLESS(
            ret == 0,
            "unable to initialize context: %s",
            LastSystemErrorText(-ret));

#if defined(AIO_RING_MAGIC)
        auto* ring = reinterpret_cast<io_ring*>(Context);
        Y_ABORT_UNLESS(
            ring->magic == AIO_RING_MAGIC &&
                ring->header_length == sizeof(io_ring),
            "unsupported AIO version");
#endif   // AIO_RING_MAGIC
    }

    ~TAsyncIOContext()
    {
        if (Context) {
            int ret = io_destroy(Context);
            Y_ABORT_UNLESS(
                ret == 0,
                "unable to destroy context: %s",
                LastSystemErrorText(-ret));
        }
    }

    void Submit(iocb* io)
    {
        int ret = io_submit(Context, 1, &io);
        if (Y_UNLIKELY(ret < 0)) {
            ythrow TSystemError(-ret) << "unable to submit async IO operation";
        }
    }

    int GetEvents(int nr, io_event* events)
    {
#if defined(AIO_RING_MAGIC)
        auto* ring = reinterpret_cast<io_ring*>(Context);

        int count = 0;
        while (count < nr) {
            ui32 tail;
            __atomic_load(&ring->tail, &tail, __ATOMIC_ACQUIRE);

            ui32 head = ring->head;
            if (head == tail) {
                break;
            }

            events[count++] = ring->events[head];

            head = (head + 1) % ring->nr;
            __atomic_store(&ring->head, &head, __ATOMIC_RELEASE);
        }

        return count;

#else    // !AIO_RING_MAGIC

        int ret = io_getevents(Context, 0, nr, events, nullptr);
        Y_ABORT_UNLESS(
            ret >= 0,
            "unable to get async IO completion: %s",
            LastSystemErrorText(-ret));

        return ret;
#endif   // AIO_RING_MAGIC
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TAsyncIORequest: iocb
{
    TCallContextPtr CallContext;

    TGuardedSgList GuardedSgList;
    TGuardedSgList::TGuard Guard;

    ui64 Offset;
    ui64 Length;

    TPromise<NProto::TError> Result;

    TAsyncIORequest(
        TCallContextPtr callContext,
        TGuardedSgList guardedSgList,
        ui64 offset,
        ui64 length,
        TPromise<NProto::TError> result)
        : CallContext(std::move(callContext))
        , GuardedSgList(std::move(guardedSgList))
        , Guard(GuardedSgList.Acquire())
        , Offset(offset)
        , Length(length)
        , Result(std::move(result))
    {}

    void HandleCompletion()
    {
        Result.SetValue({});
    }

    void HandleError(int error)
    {
        Result.SetValue(MakeError(
            MAKE_SYSTEM_ERROR(error),
            TStringBuilder() << "(" << LastSystemErrorText(error) << ") "
                             << "async IO operation failed"));
    }
};

using TAsyncIORequestPtr = std::unique_ptr<TAsyncIORequest>;

////////////////////////////////////////////////////////////////////////////////

class TLocalStorage final: public IStorage
{
private:
    TFileHandle FileHandle;
    ui64 FileSize;
    ui32 BlockSize;

    TAsyncIOContext IOContext;

    TThread PollerThread;
    TAtomic ShouldStop = 0;

public:
    TLocalStorage(const TString& path, ui32 blockSize, ui32 blocksCount)
        : FileHandle(path, OpenAlways | RdWr | DirectAligned | Sync)
        , FileSize(static_cast<ui64>(blocksCount) * blockSize)
        , BlockSize(blockSize)
        , IOContext(MAX_EVENTS_BATCH)
        , PollerThread(ThreadProc, this)
    {
        FileHandle.Resize(FileSize);
    }

    void Start() override
    {
        PollerThread.Start();
    }

    void Stop() override
    {
        AtomicSet(ShouldStop, 1);
        PollerThread.Join();
    }

    TFuture<NProto::TError> ReadBlocks(
        TCallContextPtr callContext,
        TReadBlocksRequestPtr request,
        TGuardedSgList guardedSgList) override
    {
        ui64 offset = static_cast<ui64>(request->GetBlockIndex()) * BlockSize;
        ui64 length = static_cast<ui64>(request->GetBlocksCount()) * BlockSize;

        auto response = NewPromise<NProto::TError>();

        auto req = std::make_unique<TAsyncIORequest>(
            std::move(callContext),
            std::move(guardedSgList),
            offset,
            length,
            response);

        Y_ABORT_UNLESS(req->Guard);
        const auto& sglist = req->Guard.Get();

        io_prep_preadv(
            req.get(),
            FileHandle,
            (iovec*)sglist.begin(),
            sglist.size(),
            offset);

        Submit(std::move(req));
        return response;
    }

    TFuture<NProto::TError> WriteBlocks(
        TCallContextPtr callContext,
        TWriteBlocksRequestPtr request,
        TGuardedSgList guardedSgList) override
    {
        ui64 offset = static_cast<ui64>(request->GetBlockIndex()) * BlockSize;
        ui64 length = static_cast<ui64>(request->GetBlocksCount()) * BlockSize;

        auto response = NewPromise<NProto::TError>();

        auto req = std::make_unique<TAsyncIORequest>(
            std::move(callContext),
            std::move(guardedSgList),
            offset,
            length,
            response);

        Y_ABORT_UNLESS(req->Guard);
        const auto& sglist = req->Guard.Get();

        io_prep_pwritev(
            req.get(),
            FileHandle,
            (iovec*)sglist.begin(),
            sglist.size(),
            offset);

        Submit(std::move(req));
        return response;
    }

private:
    void Submit(TAsyncIORequestPtr request)
    {
        LWTRACK(
            IORequestStarted,
            request->CallContext->LWOrbit,
            request->CallContext->RequestId);

        IOContext.Submit(request.get());

        request.release();   // ownership transferred
    }

    static void* ThreadProc(void* arg)
    {
        reinterpret_cast<TLocalStorage*>(arg)->Run();
        return nullptr;
    }

    void Run()
    {
        SetHighestThreadPriority();
        NCloud::SetCurrentThreadName("IO");

        TSpinWait sw;
        sw.T = WAIT_TIMEOUT;

        io_event event = {};
        while (AtomicGet(ShouldStop) == 0) {
            if (IOContext.GetEvents(1, &event) == 1) {
                TAsyncIORequestPtr request(
                    static_cast<TAsyncIORequest*>(event.obj));

                LWTRACK(
                    IORequestCompleted,
                    request->CallContext->LWOrbit,
                    request->CallContext->RequestId);

                signed res = event.res;   // it is really signed
                if (res == (signed)request->Length) {
                    request->HandleCompletion();
                } else {
                    request->HandleError(-res);
                }

                // reset spin wait
                sw.T = WAIT_TIMEOUT;
                sw.C = 0;
            } else {
                sw.Sleep();
            }
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStoragePtr
CreateLocalAIOStorage(const TString& path, ui32 blockSize, ui32 blocksCount)
{
    return std::make_shared<TLocalStorage>(path, blockSize, blocksCount);
}

}   // namespace NCloud::NBlockStore
