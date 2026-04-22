#include "buffer.h"

#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/size_literals.h>
#include <util/generic/vector.h>
#include <util/system/info.h>

namespace NCloud::NStorage::NRdma {

using testing::_;
using testing::Invoke;

////////////////////////////////////////////////////////////////////////////////

namespace {

int DeleteHeapMockMr(ibv_mr* mr)
{
    delete mr;
    return 0;
}

class TVerbsMock: public NVerbs::IVerbs
{
public:
    TVerbsMock() = default;
    ~TVerbsMock() override = default;

    MOCK_METHOD(
        NVerbs::TContextPtr,
        OpenDevice,
        (ibv_device * device),
        (override));
    MOCK_METHOD(NVerbs::TDeviceListPtr, GetDeviceList, (), (override));

    MOCK_METHOD(
        NVerbs::TProtectionDomainPtr,
        CreateProtectionDomain,
        (ibv_context * context),
        (override));
    MOCK_METHOD(
        NVerbs::TMemoryRegionPtr,
        RegisterMemoryRegion,
        (ibv_pd * pd, void* addr, size_t length, int flags),
        (override));

    MOCK_METHOD(
        NVerbs::TCompletionChannelPtr,
        CreateCompletionChannel,
        (ibv_context * context),
        (override));
    MOCK_METHOD(
        NVerbs::TCompletionQueuePtr,
        CreateCompletionQueue,
        (ibv_context * context,
         int cqe,
         void* cq_context,
         ibv_comp_channel* channel,
         int comp_vector),
        (override));

    MOCK_METHOD(
        void,
        RequestCompletionEvent,
        (ibv_cq * cq, int solicitedOnly),
        (override));
    MOCK_METHOD(void*, GetCompletionEvent, (ibv_cq * cq), (override));
    MOCK_METHOD(
        void,
        AckCompletionEvents,
        (ibv_cq * cq, unsigned int count),
        (override));

    MOCK_METHOD(
        bool,
        PollCompletionQueue,
        (ibv_cq * cq, NVerbs::ICompletionHandler* handler),
        (override));

    MOCK_METHOD(void, PostSend, (ibv_qp * qp, ibv_send_wr* wr), (override));
    MOCK_METHOD(void, PostRecv, (ibv_qp * qp, ibv_recv_wr* wr), (override));

    MOCK_METHOD(
        NVerbs::TAddressInfoPtr,
        GetAddressInfo,
        (const TString& host, ui32 port, rdma_addrinfo* hints),
        (override));

    MOCK_METHOD(NVerbs::TEventChannelPtr, CreateEventChannel, (), (override));
    MOCK_METHOD(
        NVerbs::TConnectionEventPtr,
        GetConnectionEvent,
        (rdma_event_channel * channel),
        (override));

    MOCK_METHOD(
        NVerbs::TConnectionPtr,
        CreateConnection,
        (rdma_event_channel * channel,
         void* context,
         rdma_port_space ps,
         ui8 tos),
        (override));
    MOCK_METHOD(
        void,
        BindAddress,
        (rdma_cm_id * id, sockaddr* addr),
        (override));
    MOCK_METHOD(
        void,
        ResolveAddress,
        (rdma_cm_id * id, sockaddr* src, sockaddr* dst, TDuration timeout),
        (override));
    MOCK_METHOD(
        void,
        ResolveRoute,
        (rdma_cm_id * id, TDuration timeout),
        (override));
    MOCK_METHOD(TString, GetPeer, (rdma_cm_id * id), (override));
    MOCK_METHOD(void, Listen, (rdma_cm_id * id, int backlog), (override));
    MOCK_METHOD(
        void,
        Connect,
        (rdma_cm_id * id, rdma_conn_param* param),
        (override));
    MOCK_METHOD(void, Disconnect, (rdma_cm_id * id), (override));
    MOCK_METHOD(
        void,
        Accept,
        (rdma_cm_id * id, rdma_conn_param* param),
        (override));
    MOCK_METHOD(
        void,
        Reject,
        (rdma_cm_id * id, const void* private_data, ui8 private_data_len),
        (override));
    MOCK_METHOD(
        void,
        RdmaCreateQP,
        (rdma_cm_id * id, ibv_qp_init_attr* attr),
        (override));
    MOCK_METHOD(void, RdmaDestroyQP, (rdma_cm_id * id), (override));
    MOCK_METHOD(
        ibv_qp*,
        CreateQP,
        (ibv_pd * pd, ibv_qp_init_attr* attr),
        (override));
    MOCK_METHOD(void, DestroyQP, (ibv_qp * qp), (override));
    MOCK_METHOD(
        void,
        ModifyQP,
        (ibv_qp * qp, ibv_qp_attr* attr, int mask),
        (override));
};

class TBufferPoolTest: public ::testing::Test
{
protected:
    std::shared_ptr<TVerbsMock> Verbs{std::make_shared<TVerbsMock>()};
    TBufferPool Pool{TBufferPoolConfig{}};

public:
    TBufferPoolTest() = default;
    ~TBufferPoolTest() override = default;
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST_F(TBufferPoolTest, ShouldAllocateAlignedBuffers)
{
    Pool.Init(Verbs, nullptr, 0);

    auto buffer = Pool.AcquireBuffer(1234);
    EXPECT_EQ(buffer.Length, NSystemInfo::GetPageSize());
}

TEST_F(TBufferPoolTest, ShouldAllocateSmallBuffersInOneChunk)
{
    Pool.Init(Verbs, nullptr, 0);

    const auto& stats = Pool.GetStats();

    auto buffer = Pool.AcquireBuffer(4_KB);
    EXPECT_EQ(buffer.Length, NSystemInfo::GetPageSize());

    EXPECT_EQ(stats.ActiveChunksCount, 1u);
    EXPECT_EQ(stats.CustomChunksCount, 0u);
    EXPECT_EQ(stats.FreeChunksCount, 0u);

    Pool.ReleaseBuffer(buffer);

    EXPECT_EQ(stats.ActiveChunksCount, 1u);
    EXPECT_EQ(stats.CustomChunksCount, 0u);
    EXPECT_EQ(stats.FreeChunksCount, 0u);
}

TEST_F(TBufferPoolTest, ShouldAllocateCustomChunkForLargeBuffer)
{
    Pool.Init(Verbs, nullptr, 0);

    const auto& stats = Pool.GetStats();

    auto buffer = Pool.AcquireBuffer(4_MB);
    EXPECT_EQ(buffer.Length, 4_MB);

    EXPECT_EQ(stats.ActiveChunksCount, 0u);
    EXPECT_EQ(stats.CustomChunksCount, 1u);
    EXPECT_EQ(stats.FreeChunksCount, 0u);

    Pool.ReleaseBuffer(buffer);

    EXPECT_EQ(stats.ActiveChunksCount, 0u);
    EXPECT_EQ(stats.CustomChunksCount, 0u);
    EXPECT_EQ(stats.FreeChunksCount, 0u);
}

TEST_F(TBufferPoolTest, ShouldCacheFreeChunks)
{
    Pool.Init(Verbs, nullptr, 0);

    const auto& stats = Pool.GetStats();

    TVector<TPooledBuffer> buffers = {
        Pool.AcquireBuffer(1_MB),
        Pool.AcquireBuffer(1_MB),
        Pool.AcquireBuffer(1_MB),
        Pool.AcquireBuffer(1_MB),
        // trigger chunk switching
        Pool.AcquireBuffer(1_MB),
    };

    EXPECT_EQ(stats.ActiveChunksCount, 2u);
    EXPECT_EQ(stats.CustomChunksCount, 0u);
    EXPECT_EQ(stats.FreeChunksCount, 0u);

    for (auto& buffer: buffers) {
        Pool.ReleaseBuffer(buffer);
    }

    EXPECT_EQ(stats.ActiveChunksCount, 1u);
    EXPECT_EQ(stats.CustomChunksCount, 0u);
    EXPECT_EQ(stats.FreeChunksCount, 1u);
}

TEST_F(TBufferPoolTest, ShouldSetLkeyAndRkeyToBuffers)
{
    ibv_pd pd{.context = nullptr, .handle = 123};
    Pool.Init(Verbs, &pd, 0);

    constexpr ui32 Lkey = 1;
    constexpr ui32 Rkey = 2;
    EXPECT_CALL(*Verbs, RegisterMemoryRegion(&pd, _, 4_MB, 0))
        .WillOnce(Invoke(
            [](ibv_pd* pd, void* addr, size_t length, int flags)
            {
                Y_UNUSED(pd, flags);
                auto mr =
                    NVerbs::TMemoryRegionPtr(new ibv_mr(), DeleteHeapMockMr);
                mr->lkey = Lkey;
                mr->rkey = Rkey;
                mr->addr = addr;
                mr->length = length;
                return mr;
            }));

    const auto& stats = Pool.GetStats();
    auto buffer1 = Pool.AcquireBuffer(4_KB);
    auto buffer2 = Pool.AcquireBuffer(4_KB);

    EXPECT_EQ(buffer1.LKey, Lkey);
    EXPECT_EQ(buffer1.RKey, Rkey);
    EXPECT_EQ(buffer2.LKey, Lkey);
    EXPECT_EQ(buffer2.RKey, Rkey);

    EXPECT_EQ(stats.ActiveChunksCount, 1u);
    EXPECT_EQ(stats.CustomChunksCount, 0u);
    EXPECT_EQ(stats.FreeChunksCount, 0u);

    Pool.ReleaseBuffer(buffer1);
    Pool.ReleaseBuffer(buffer2);

    EXPECT_EQ(stats.ActiveChunksCount, 1u);
    EXPECT_EQ(stats.CustomChunksCount, 0u);
    EXPECT_EQ(stats.FreeChunksCount, 0u);

    Pool.Init(Verbs, nullptr, 0);
}

}   // namespace NCloud::NStorage::NRdma
