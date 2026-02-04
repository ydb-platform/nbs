#include "shm_client.h"

#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/public/api/protos/data.pb.h>
#include <cloud/filestore/public/api/protos/server.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/folder/path.h>
#include <util/generic/string.h>
#include <util/system/file.h>
#include <util/thread/lfqueue.h>

#include <sys/mman.h>

namespace NCloud::NFileStore::NLoadTest {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration PingInterval = TDuration::Seconds(10);

////////////////////////////////////////////////////////////////////////////////

class TSharedMemoryClient final
    : public IShmDataClient
    , public std::enable_shared_from_this<TSharedMemoryClient>
{
private:
    const TString FullPath;
    const ui64 ShmSize;
    const ui64 SlotSize;

    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;

    // Control transport for SHM RPCs (Mmap / Munmap / PingMmapRegion).
    const IShmControlPtr ShmControl;

    TLog Log;

    void* LocalAddr = MAP_FAILED;
    ui64 RegionId = 0;

    TLockFreeQueue<ui64> FreeOffsets;

public:
    TSharedMemoryClient(
            TString fullPath,
            ui64 shmSize,
            ui64 slotSize,
            IShmControlPtr shmControl,
            ISchedulerPtr scheduler,
            ITimerPtr timer,
            ILoggingServicePtr logging)
        : FullPath(std::move(fullPath))
        , ShmSize(shmSize)
        , SlotSize(slotSize)
        , Scheduler(std::move(scheduler))
        , Timer(std::move(timer))
        , ShmControl(std::move(shmControl))
    {
        Log = logging->CreateLog("NFS_SHM_CLIENT");
    }

    void Start() override
    {
        for (ui64 offset = 0; offset < ShmSize; offset += SlotSize) {
            FreeOffsets.Enqueue(offset);
        }

        ShmControl->Start();
        SetupSharedMemory();
        SchedulePing();
    }

    void Stop() override
    {
        TeardownSharedMemory();
        ShmControl->Stop();
    }

    ui64 PrepareWrite(NProto::TWriteDataRequest& request) override
    {
        Y_ABORT_UNLESS(LocalAddr != MAP_FAILED, "PrepareWrite called before Start()");
        const auto& buffer = request.GetBuffer();
        const ui64 len = buffer.size();
        Y_ABORT_UNLESS(
            len <= SlotSize,
            "buffer size %lu exceeds slot size %lu",
            len,
            SlotSize);
        const ui64 shmOffset = AllocateShmOffset();

        memcpy(static_cast<char*>(LocalAddr) + shmOffset, buffer.data(), len);
        request.ClearBuffer();

        auto* iovec = request.AddIovecs();
        iovec->SetBase(shmOffset);
        iovec->SetLength(len);
        request.SetRegionId(RegionId);

        return shmOffset;
    }

    char* PrepareRead(NProto::TReadDataRequest& request, ui64& outOffset) override
    {
        Y_ABORT_UNLESS(LocalAddr != MAP_FAILED, "PrepareRead called before Start()");

        const ui64 len = request.GetLength();
        Y_ABORT_UNLESS(
            len <= SlotSize,
            "request length %lu exceeds slot size %lu",
            len,
            SlotSize);
        outOffset = AllocateShmOffset();

        auto* iovec = request.AddIovecs();
        iovec->SetBase(outOffset);
        iovec->SetLength(len);
        request.SetRegionId(RegionId);

        return static_cast<char*>(LocalAddr) + outOffset;
    }

    void FreeOffset(ui64 offset) override
    {
        if (offset != Max<ui64>()) {
            FreeOffsets.Enqueue(offset);
        }
    }

private:
    void SetupSharedMemory()
    {
        TFile file(FullPath, CreateAlways | RdWr);
        file.Resize(ShmSize);
        int fd = file.GetHandle();

        LocalAddr =
            ::mmap(nullptr, ShmSize, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (LocalAddr == MAP_FAILED) {
            ythrow TSystemError() << "mmap failed for " << FullPath;
        }
        file.Close();

        auto ctx = MakeIntrusive<TCallContext>();
        auto req = std::make_shared<NProto::TMmapRequest>();
        req->SetFilePath(TFsPath(FullPath).GetName());
        req->SetSize(ShmSize);
        req->SetPageSize(SlotSize);

        auto response = ShmControl->Mmap(std::move(ctx), std::move(req)).GetValueSync();
        if (HasError(response)) {
            ythrow TServiceError(response.GetError()) << "Mmap RPC failed";
        }

        RegionId = response.GetId();

        STORAGE_INFO(
            "Shared memory region registered: id=" << RegionId
            << ", file=" << FullPath
            << ", size=" << ShmSize);
    }

    void TeardownSharedMemory()
    {
        if (RegionId != 0) {
            auto ctx = MakeIntrusive<TCallContext>();
            auto req = std::make_shared<NProto::TMunmapRequest>();
            req->SetId(RegionId);
            ShmControl->Munmap(std::move(ctx), std::move(req)).Wait();
            RegionId = 0;
        }

        if (LocalAddr != MAP_FAILED) {
            ::munmap(LocalAddr, ShmSize);
            LocalAddr = MAP_FAILED;
        }
    }

    void SchedulePing()
    {
        Scheduler->Schedule(
            Timer->Now() + PingInterval,
            [weakSelf = weak_from_this()] {
                if (auto self = weakSelf.lock()) {
                    self->PingRegion();
                }
            });
    }

    void PingRegion()
    {
        if (RegionId == 0) {
            return;
        }

        auto ctx = MakeIntrusive<TCallContext>();
        auto req = std::make_shared<NProto::TPingMmapRegionRequest>();
        req->SetId(RegionId);

        ShmControl->PingMmapRegion(std::move(ctx), std::move(req))
            .Subscribe([weakSelf = weak_from_this()](
                           const TFuture<NProto::TPingMmapRegionResponse>&) {
                if (auto self = weakSelf.lock()) {
                    self->SchedulePing();
                }
            });
    }

    ui64 AllocateShmOffset()
    {
        ui64 slot = 0;
        if (!FreeOffsets.Dequeue(&slot)) {
            ythrow yexception()
                << "no free SHM slots available for the loadtest";
        }
        return slot;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IShmDataClientPtr CreateSharedMemoryClient(
    TString fullFilePath,
    ui64 shmSize,
    ui64 slotSize,
    IShmControlPtr shmControl,
    ISchedulerPtr scheduler,
    ITimerPtr timer,
    ILoggingServicePtr logging)
{
    return std::make_shared<TSharedMemoryClient>(
        std::move(fullFilePath),
        shmSize,
        slotSize,
        std::move(shmControl),
        std::move(scheduler),
        std::move(timer),
        std::move(logging));
}

}   // namespace NCloud::NFileStore::NLoadTest
