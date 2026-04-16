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

class TSharedMemoryClientBase
    : public IFileStoreService
{
protected:
    const IFileStoreServicePtr Inner_;

public:
    explicit TSharedMemoryClientBase(IFileStoreServicePtr inner)
        : Inner_(std::move(inner))
    {}

    void Start() override
    {
        Inner_->Start();
    }

    void Stop() override
    {
        Inner_->Stop();
    }

    // Forward all control and data service methods to Inner_
#define FORWARD_METHOD(name, ...)                                              \
    TFuture<NProto::T##name##Response> name(                                   \
        TCallContextPtr callContext,                                            \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return Inner_->name(                                                   \
            std::move(callContext),                                            \
            std::move(request));                                               \
    }                                                                          \
// FORWARD_METHOD

    FILESTORE_CONTROL_SERVICE(FORWARD_METHOD)
    FILESTORE_DATA_SERVICE(FORWARD_METHOD)

#undef FORWARD_METHOD

    void GetSessionEventsStream(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TGetSessionEventsRequest> request,
        IResponseHandlerPtr<NProto::TGetSessionEventsResponse> handler) override
    {
        Inner_->GetSessionEventsStream(
            std::move(callContext),
            std::move(request),
            std::move(handler));
    }

    TFuture<NProto::TReadDataLocalResponse> ReadDataLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataLocalRequest> request) override
    {
        return Inner_->ReadDataLocal(
            std::move(callContext),
            std::move(request));
    }

    TFuture<NProto::TWriteDataLocalResponse> WriteDataLocal(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataLocalRequest> request) override
    {
        return Inner_->WriteDataLocal(
            std::move(callContext),
            std::move(request));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TSharedMemoryClient final
    : public TSharedMemoryClientBase
    , public std::enable_shared_from_this<TSharedMemoryClient>
{
private:
    const TString FullPath_;
    const ui64 ShmSize_;
    const ui64 SlotSize_;
    const ui64 NumSlots_;

    const ISchedulerPtr Scheduler_;
    const ITimerPtr Timer_;

    // Control transport for SHM RPCs (Mmap / Munmap / PingMmapRegion).
    const IShmControlPtr ShmControl_;

    // Session-aware data transport: WriteData/ReadData go through this so that
    // session headers (sessionId, seqNo) are properly filled in.
    const std::shared_ptr<IFileStore> DataOps_;

    TLog Log;

    void* LocalAddr_ = MAP_FAILED;
    ui64 RegionId_ = 0;

    std::atomic<ui64> SlotCounter_{0};

public:
    TSharedMemoryClient(
            TString fullPath,
            ui64 shmSize,
            ui64 slotSize,
            IFileStoreServicePtr inner,
            IShmControlPtr shmControl,
            std::shared_ptr<IFileStore> dataOps,
            ISchedulerPtr scheduler,
            ITimerPtr timer,
            ILoggingServicePtr logging)
        : TSharedMemoryClientBase(std::move(inner))
        , FullPath_(std::move(fullPath))
        , ShmSize_(shmSize)
        , SlotSize_(slotSize)
        , NumSlots_(slotSize ? shmSize / slotSize : 1)
        , Scheduler_(std::move(scheduler))
        , Timer_(std::move(timer))
        , ShmControl_(std::move(shmControl))
        , DataOps_(std::move(dataOps))
    {
        Log = logging->CreateLog("NFS_SHM_CLIENT");
    }

    void Start() override
    {
        Inner_->Start();
        SetupSharedMemory();
        SchedulePing();
    }

    void Stop() override
    {
        TeardownSharedMemory();
        Inner_->Stop();
    }

    TFuture<NProto::TWriteDataResponse> WriteData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDataRequest> request) override
    {
        const auto& buffer = request->GetBuffer();
        if (buffer.empty() || LocalAddr_ == MAP_FAILED) {
            // No buffer or shm not ready: fall through to inner directly
            return Inner_->WriteData(std::move(callContext), std::move(request));
        }

        const ui64 len = buffer.size();
        const ui64 shmOffset = AllocateShmSlot(len);

        memcpy(static_cast<char*>(LocalAddr_) + shmOffset, buffer.data(), len);
        request->ClearBuffer();

        auto* iovec = request->AddIovecs();
        iovec->SetBase(shmOffset);
        iovec->SetLength(len);
        request->SetRegionId(RegionId_);

        return DataOps_->WriteData(std::move(callContext), std::move(request));
    }

    TFuture<NProto::TReadDataResponse> ReadData(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TReadDataRequest> request) override
    {
        if (LocalAddr_ == MAP_FAILED) {
            return Inner_->ReadData(std::move(callContext), std::move(request));
        }

        const ui64 len = request->GetLength();
        const ui64 shmOffset = AllocateShmSlot(len);

        auto* iovec = request->AddIovecs();
        iovec->SetBase(shmOffset);
        iovec->SetLength(len);
        request->SetRegionId(RegionId_);

        char* localBase = static_cast<char*>(LocalAddr_);

        return DataOps_->ReadData(std::move(callContext), std::move(request))
            .Apply([localBase, shmOffset](
                       const TFuture<NProto::TReadDataResponse>& f) {
                auto response = f.GetValue();
                if (response.GetBuffer().empty() && response.GetLength() > 0) {
                    // Data was written into shared memory by the server
                    const ui64 dataLen = response.GetLength();
                    response.SetBuffer(TString(localBase + shmOffset, dataLen));
                }
                return response;
            });
    }

private:
    void SetupSharedMemory()
    {
        // Create and size the file
        TFile file(FullPath_, CreateAlways | RdWr);
        file.Resize(ShmSize_);
        int fd = file.GetHandle();

        LocalAddr_ =
            ::mmap(nullptr, ShmSize_, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        if (LocalAddr_ == MAP_FAILED) {
            ythrow TSystemError() << "mmap failed for " << FullPath_;
        }
        // file descriptor can be closed after mmap
        file.Close();

        // Register region with the server
        auto ctx = MakeIntrusive<TCallContext>();
        auto req = std::make_shared<NProto::TMmapRequest>();
        req->SetFilePath(TFsPath(FullPath_).GetName());
        req->SetSize(ShmSize_);

        auto response = ShmControl_->Mmap(std::move(ctx), std::move(req)).GetValueSync();
        if (HasError(response)) {
            ythrow TServiceError(response.GetError()) << "Mmap RPC failed";
        }

        RegionId_ = response.GetId();

        STORAGE_INFO(
            "Shared memory region registered: id=" << RegionId_
            << ", file=" << FullPath_
            << ", size=" << ShmSize_);
    }

    void TeardownSharedMemory()
    {
        if (RegionId_ != 0) {
            auto ctx = MakeIntrusive<TCallContext>();
            auto req = std::make_shared<NProto::TMunmapRequest>();
            req->SetId(RegionId_);
            ShmControl_->Munmap(std::move(ctx), std::move(req)).Wait();
            RegionId_ = 0;
        }

        if (LocalAddr_ != MAP_FAILED) {
            ::munmap(LocalAddr_, ShmSize_);
            LocalAddr_ = MAP_FAILED;
        }
    }

    void SchedulePing()
    {
        Scheduler_->Schedule(
            Timer_->Now() + PingInterval,
            [weakSelf = weak_from_this()] {
                if (auto self = weakSelf.lock()) {
                    self->PingRegion();
                }
            });
    }

    void PingRegion()
    {
        if (RegionId_ == 0) {
            return;
        }

        auto ctx = MakeIntrusive<TCallContext>();
        auto req = std::make_shared<NProto::TPingMmapRegionRequest>();
        req->SetId(RegionId_);

        ShmControl_->PingMmapRegion(std::move(ctx), std::move(req))
            .Subscribe([weakSelf = weak_from_this()](
                           const TFuture<NProto::TPingMmapRegionResponse>&) {
                if (auto self = weakSelf.lock()) {
                    self->SchedulePing();
                }
            });
    }

    ui64 AllocateShmSlot(ui64 /*size*/)
    {
        if (NumSlots_ == 0) {
            return 0;
        }
        ui64 slot =
            SlotCounter_.fetch_add(1, std::memory_order_relaxed) % NumSlots_;
        return slot * SlotSize_;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IFileStoreServicePtr CreateSharedMemoryClient(
    TString fullFilePath,
    ui64 shmSize,
    ui64 slotSize,
    IFileStoreServicePtr inner,
    IShmControlPtr shmControl,
    std::shared_ptr<IFileStore> dataOps,
    ISchedulerPtr scheduler,
    ITimerPtr timer,
    ILoggingServicePtr logging)
{
    return std::make_shared<TSharedMemoryClient>(
        std::move(fullFilePath),
        shmSize,
        slotSize,
        std::move(inner),
        std::move(shmControl),
        std::move(dataOps),
        std::move(scheduler),
        std::move(timer),
        std::move(logging));
}

}   // namespace NCloud::NFileStore::NLoadTest
