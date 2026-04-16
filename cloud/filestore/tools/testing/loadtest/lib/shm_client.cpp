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

#include <atomic>
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
    const ui64 NumSlots;

    const ISchedulerPtr Scheduler;
    const ITimerPtr Timer;

    // Control transport for SHM RPCs (Mmap / Munmap / PingMmapRegion).
    const IShmControlPtr ShmControl;

    TLog Log;

    void* LocalAddr = MAP_FAILED;
    ui64 RegionId = 0;

    std::atomic<ui64> SlotCounter{0};

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
        , NumSlots(slotSize ? shmSize / slotSize : 1)
        , Scheduler(std::move(scheduler))
        , Timer(std::move(timer))
        , ShmControl(std::move(shmControl))
    {
        Log = logging->CreateLog("NFS_SHM_CLIENT");
    }

    void Start() override
    {
        ShmControl->Start();
        SetupSharedMemory();
        SchedulePing();
    }

    void Stop() override
    {
        TeardownSharedMemory();
        ShmControl->Stop();
    }

    void PrepareWrite(NProto::TWriteDataRequest& request) override
    {
        const auto& buffer = request.GetBuffer();
        if (buffer.empty() || LocalAddr == MAP_FAILED) {
            return;
        }

        const ui64 len = buffer.size();
        Y_ABORT_UNLESS(
            len <= SlotSize,
            "buffer size %lu exceeds slot size %lu",
            len,
            SlotSize);
        const ui64 shmOffset = AllocateShmSlot();

        memcpy(static_cast<char*>(LocalAddr) + shmOffset, buffer.data(), len);
        request.ClearBuffer();

        auto* iovec = request.AddIovecs();
        iovec->SetBase(shmOffset);
        iovec->SetLength(len);
        request.SetRegionId(RegionId);
    }

    char* PrepareRead(NProto::TReadDataRequest& request) override
    {
        if (LocalAddr == MAP_FAILED) {
            return nullptr;
        }

        const ui64 len = request.GetLength();
        Y_ABORT_UNLESS(
            len <= SlotSize,
            "request length %lu exceeds slot size %lu",
            len,
            SlotSize);
        const ui64 shmOffset = AllocateShmSlot();

        auto* iovec = request.AddIovecs();
        iovec->SetBase(shmOffset);
        iovec->SetLength(len);
        request.SetRegionId(RegionId);

        return static_cast<char*>(LocalAddr) + shmOffset;
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

    ui64 AllocateShmSlot()
    {
        if (NumSlots == 0) {
            return 0;
        }
        ui64 slot =
            SlotCounter.fetch_add(1, std::memory_order_relaxed) % NumSlots;
        return slot * SlotSize;
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
