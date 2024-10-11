#include "vhost-client.h"

#include "monotonic_buffer_resource.h"
#include "vhost-queue.h"
#include "vhost-user-protocol/msg-get-feature.h"
#include "vhost-user-protocol/msg-get-protocol-feature.h"
#include "vhost-user-protocol/msg-get-queue-num.h"
#include "vhost-user-protocol/msg-get-vring-base.h"
#include "vhost-user-protocol/msg-set-feature.h"
#include "vhost-user-protocol/msg-set-mem-table.h"
#include "vhost-user-protocol/msg-set-owner.h"
#include "vhost-user-protocol/msg-set-protocol-feauture.h"
#include "vhost-user-protocol/msg-set-vring-addr.h"
#include "vhost-user-protocol/msg-set-vring-base.h"
#include "vhost-user-protocol/msg-set-vring-call.h"
#include "vhost-user-protocol/msg-set-vring-err.h"
#include "vhost-user-protocol/msg-set-vring-kick.h"
#include "vhost-user-protocol/msg-set-vring-num.h"

#include <util/generic/string.h>
#include <util/string/printf.h>

#include <thread>

#include <unistd.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/un.h>

namespace NVHost {

using namespace NThreading;

////////////////////////////////////////////////////////////////////////////////

TClient::TClient(TString sockPath, TClientParams params)
    : SockPath(std::move(sockPath))
    , Params(params)
    , Logger(CreateLogBackend("console", TLOG_INFO))
{}

TClient::~TClient()
{
    DeInit();
}

bool TClient::Execute(NVHostUser::IMessage& msg)
{
    auto requestStr = msg.ToString();
    if(!msg.Execute(Sock)) {
        Logger.AddLog(
            ELogPriority::TLOG_ERR,
            "TClient::%s: error %s. Request %s\n",
            __FUNCTION__,
            strerror(errno),
            requestStr.c_str());
        return false;
    }
    Logger.AddLog(
        ELogPriority::TLOG_DEBUG,
        "TClient::%s: Request %s OK\n",
        __FUNCTION__,
        requestStr.c_str());
    return true;
}

bool TClient::Connect()
{
    TFileHandle sock { socket(AF_UNIX, SOCK_STREAM, 0) };

    if (!sock) {
        const int ec = errno;
        Logger.AddLog(
            ELogPriority::TLOG_ERR,
            "TClient::%s: Socket error. Error code %d\n",
            __FUNCTION__,
            ec);
        return false;
    }

    struct sockaddr_un un;
    un.sun_family = AF_UNIX;
    strlcpy(un.sun_path, SockPath.c_str(), sizeof(un.sun_path));

    if (int errorCode = connect(
        sock,
        reinterpret_cast<sockaddr*>(&un),
        sizeof(un.sun_family) + SockPath.size()) == -1)
    {
        Logger.AddLog(
            ELogPriority::TLOG_ERR,
            "TClient::%s: Connect error. Error code %d\n",
            __FUNCTION__,
            errorCode);
        Logger.AddLog(
            ELogPriority::TLOG_ERR,
            "errno: %d, errnostr: %s\n",
            errno,
            strerror(errno)
        );
        return false;
    }

    Sock.Swap(sock);

    return true;
}

bool TClient::CoordinationFeatures(
    uint64_t virtioFeatures,
    uint64_t virtioProtocolFeatures)
{
    NVHostUser::TGetFeature getFuatureMsg;
    if (!Execute(getFuatureMsg)) {
        return false;
    }

    if (!BitIsSet(
        getFuatureMsg.GetResult(),
        NVHostUser::TGetFeature::VHOST_USER_F_PROTOCOL_FEATURES))
    {
        Logger.AddLog(
            ELogPriority::TLOG_ERR,
            "TClient::%s: Coordination error VHOST_USER_F_PROTOCOL_FEATURES\n",
            __FUNCTION__);
        return false;
    }

    NVHostUser::TSetFeature setFeatureMsg(virtioFeatures);
    if (!Execute(setFeatureMsg)) {
        return false;
    }

    NVHostUser::TGetProtocolFeature getProtocolFeature;
    if (!Execute(getProtocolFeature)) {
        return false;
    }

    NVHostUser::TSetProtocolFeature setProtocolFeatureMsg(virtioProtocolFeatures);
    if (!Execute(setProtocolFeatureMsg)) {
        return false;
    }

    return true;
}

auto TClient::MapMemory(uint64_t size) -> TMemTable
{
    const int fd = syscall(__NR_memfd_create, "vhost-client-memtable", 0);
    if (fd < 0) {
        Logger.AddLog(
            ELogPriority::TLOG_ERR,
            "TClient::%s: syscall error %s\n",
            __FUNCTION__,
            strerror(errno));
        return {};
    }

    if (ftruncate(fd, size) < 0) {
        Logger.AddLog(
            ELogPriority::TLOG_ERR,
            "TClient::%s: ftruncate error %s\n",
            __FUNCTION__,
            strerror(errno));
        return {};
    }

    void* addr = mmap(
        NULL,
        size,
        PROT_READ | PROT_WRITE,
        MAP_SHARED,
        fd,
        0);

    if (addr == MAP_FAILED) {
        Logger.AddLog(
            ELogPriority::TLOG_ERR,
            "TClient::%s: mmap error %s\n",
            __FUNCTION__,
            strerror(errno));
        return {};
    }

    return { fd, static_cast<char*>(addr), size };
}

bool TClient::CoordinationMemMap()
{
    const uint64_t totalMemorySize = FastClp2(
        TotalQueuesMemory() + Params.MemorySize);

    MemTable = MapMemory(totalMemorySize);

    if (!MemTable.Size) {
        return false;
    }

    NVHostUser::TSetMemTable::MemoryRegion region {
        .GuestAddress = std::bit_cast<uint64_t>(MemTable.Addr),
        .Size = MemTable.Size,
        .UserAddress = std::bit_cast<uint64_t>(MemTable.Addr),
        .MmapOffset = 0
    };

    NVHostUser::TSetMemTable setMemTableMsg({ std::move(region)}, { MemTable.Fd });

    return Execute(setMemTableMsg);
}

uint64_t TClient::MemoryPerQueue() const
{
    return TQueue::GetQueueMemSize(Params.QueueSize);
}

uint64_t TClient::TotalQueuesMemory() const
{
    return AlignUp(Params.QueueCount * MemoryPerQueue(), Params.MemoryAlignment);
}

bool TClient::CoordinationQueue()
{
    const uint64_t memoryPerQueue = MemoryPerQueue();
    const uint64_t queueMemAlignment = 4_KB; // page

    TMonotonicBufferResource memory {MemTable.Addr, MemTable.Size};

    for (size_t i = 0; i < Params.QueueCount; ++i) {
        NVHostUser::TSetVringNum setVringNumMsg(i, Params.QueueSize);
        if (!Execute(setVringNumMsg)) {
            return false;
        }

        NVHostUser::TSetVringBase setVringBaseMsg(i, 0);
        if (!Execute(setVringBaseMsg)) {
            return false;
        }

        std::span queueMem = memory.Allocate(memoryPerQueue, queueMemAlignment);
        if (queueMem.empty()) {
            Logger.AddLog(
                ELogPriority::TLOG_ERR,
                "TClient::%s: not enough memory\n",
                __FUNCTION__);

            return false;
        }

        auto& queue = Queues.emplace_back(Params.QueueSize, queueMem);

        NVHostUser::TSetVringAddr setVringAddrMsg(
            i,
            0,  // flags
            queue.GetDescriptorsAddr(),
            queue.GetUsedRings(),
            queue.GetAvailableRingsAddr(),
            0); // logging
        if (!Execute(setVringAddrMsg)) {
            return false;
        }

        NVHostUser::TSetVringErr setVringErrMsg(i, queue.GetErrFd());
        if (!Execute(setVringErrMsg)) {
            return false;
        }

        NVHostUser::TSetVringKick setVringKickMsg(i, queue.GetKickFd());
        if (!Execute(setVringKickMsg)) {
            return false;
        }

        NVHostUser::TSetVringCall setVringCallMsg(i, queue.GetCallFd());
        if (!Execute(setVringCallMsg)) {
            return false;
        }

        if (!queue.RunOnce()) {
            return false;
        }
    }

    return true;
}

 void TClient::DeInit()
 {
    if (ShouldStop.exchange(true)) {
        return;
    }

    for (auto& q: Queues) {
        eventfd_write(q.GetCallFd(), 1);
    }

    for (auto& t: QueueThreads) {
        t.join();
    }

    if (MemTable.Fd != -1) {
        ::close(MemTable.Fd);
    }
 }

bool TClient::Init()
{
    ShouldStop = false;

    if(!Connect()) {
        return false;
    }

    uint64_t virtioFeatures = 0;
    NVHostUser::SetBit(
        virtioFeatures,
        NVHostUser::TGetFeature::VIRTIO_F_VERSION_1);

    uint64_t virtioProtocolFeatures = 0;
    NVHostUser::SetBit(
        virtioProtocolFeatures,
        NVHostUser::TGetProtocolFeature::VHOST_USER_PROTOCOL_F_MQ);

    if(!CoordinationFeatures(virtioFeatures, virtioProtocolFeatures)) {
        return false;
    }

    NVHostUser::TSetOwner setOwnerMsg;
    if (!Execute(setOwnerMsg)) {
        return false;
    }

    NVHostUser::TGetQueueNum getQueueNumMsg;
    if (!Execute(getQueueNumMsg)) {
        return false;
    }

    if (getQueueNumMsg.GetResult() != Params.QueueCount) {
        Logger.AddLog(
            ELogPriority::TLOG_ERR,
            "TClient::%s: Queue count error %lu\n",
            __FUNCTION__,
            getQueueNumMsg.GetResult());
        return false;
    }

    if (!CoordinationMemMap()) {
        return false;
    }

    if (!CoordinationQueue()){
        const int ec = errno;
        Logger.AddLog(
            ELogPriority::TLOG_ERR,
            "TClient::%s: queue error %s\n",
            __FUNCTION__,
            strerror(ec));
        return false;
    }

    QueueThreads.reserve(Queues.size());

    for (auto& q: Queues) {
        QueueThreads.emplace_back([this, &q] {
            while (!ShouldStop) {
                q.RunOnce();
            }
        });
    }

    return true;
}

TFuture<uint32_t> TClient::WriteAsync(
    uint32_t queueIndex,
    const TSgList& inBuffers,
    const TSgList& outBuffers)
{
    return Queues[queueIndex].WriteAsync(inBuffers, outBuffers);
}

std::span<char> TClient::GetMemory()
{
    return {
        MemTable.Addr + TotalQueuesMemory(),
        Params.MemorySize
    };
}

} // namespace NVHost
