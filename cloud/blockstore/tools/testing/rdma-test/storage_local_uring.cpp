#include "storage.h"

#include "probes.h"

#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/thread.h>

#include <util/system/file.h>
#include <util/system/spin_wait.h>
#include <util/system/thread.h>

#include <liburing.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

LWTRACE_USING(BLOCKSTORE_TEST_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 MAX_EVENTS_BATCH = 128;
constexpr ui32 WAIT_TIMEOUT = 10;   // us

////////////////////////////////////////////////////////////////////////////////

struct TAsyncIOContext: io_uring
{
    TAsyncIOContext(size_t nr)
    {
        int ret = io_uring_queue_init(nr, this, 0);
        Y_ABORT_UNLESS(
            ret == 0,
            "unable to initialize context: %s",
            LastSystemErrorText(-ret));
    }

    ~TAsyncIOContext()
    {
        io_uring_queue_exit(this);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TAsyncIORequest
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

        io_uring_sqe* sqe = io_uring_get_sqe(&IOContext);
        io_uring_prep_readv(
            sqe,
            FileHandle,
            (iovec*)sglist.begin(),
            sglist.size(),
            offset);

        Submit(std::move(req), sqe);
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

        io_uring_sqe* sqe = io_uring_get_sqe(&IOContext);
        io_uring_prep_writev(
            sqe,
            FileHandle,
            (iovec*)sglist.begin(),
            sglist.size(),
            offset);

        Submit(std::move(req), sqe);
        return response;
    }

private:
    void Submit(TAsyncIORequestPtr request, io_uring_sqe* sqe)
    {
        LWTRACK(
            IORequestStarted,
            request->CallContext->LWOrbit,
            request->CallContext->RequestId);

        io_uring_sqe_set_data(sqe, request.get());

        int ret = io_uring_submit(&IOContext);
        if (Y_UNLIKELY(ret < 0)) {
            ythrow TSystemError(-ret) << "unable to submit async IO operation";
        }

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

        io_uring_cqe* cqe;
        while (AtomicGet(ShouldStop) == 0) {
            int ret = io_uring_peek_cqe(&IOContext, &cqe);
            if (ret == -EAGAIN) {
                cqe = nullptr;
                ret = 0;
            }

            Y_ABORT_UNLESS(
                ret >= 0,
                "unable to get async IO completion: %s",
                LastSystemErrorText(-ret));

            if (cqe) {
                TAsyncIORequestPtr request(
                    static_cast<TAsyncIORequest*>(io_uring_cqe_get_data(cqe)));

                LWTRACK(
                    IORequestCompleted,
                    request->CallContext->LWOrbit,
                    request->CallContext->RequestId);

                signed res = cqe->res;
                io_uring_cqe_seen(&IOContext, cqe);

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
CreateLocalURingStorage(const TString& path, ui32 blockSize, ui32 blocksCount)
{
    return std::make_shared<TLocalStorage>(path, blockSize, blocksCount);
}

}   // namespace NCloud::NBlockStore
