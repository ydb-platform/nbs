#include "undelivered.h"

#include <cloud/blockstore/public/api/protos/volume.pb.h>

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/api/volume.h>

#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/test_client.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(5);

////////////////////////////////////////////////////////////////////////////////

class TUndeliveredClient
{
private:
    TTestActorRuntime& Runtime;
    TActorId Sender;

public:
    TUndeliveredClient(TTestActorRuntime& runtime)
        : Runtime(runtime)
        , Sender(runtime.AllocateEdgeActor(0))
    {}

    template <typename TResponse>
    std::unique_ptr<TResponse> TryRecvResponse(TDuration timeout = WaitTimeout)
    {
        TAutoPtr<IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TResponse>(handle, timeout);

        if (handle) {
            return std::unique_ptr<TResponse>(
                handle->Release<TResponse>().Release());
        } else {
            return nullptr;
        }
    }

    template <typename TResponse>
    std::unique_ptr<TResponse> RecvResponse()
    {
        TAutoPtr<IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TResponse>(handle, WaitTimeout);

        UNIT_ASSERT(handle);
        return std::unique_ptr<TResponse>(handle->Release<TResponse>().Release());
    }

    std::unique_ptr<TEvService::TEvStatVolumeRequest> CreateStatVolumeRequest()
    {
        auto request = std::make_unique<TEvService::TEvStatVolumeRequest>();
        return request;
    }

    std::unique_ptr<TEvService::TEvReadBlocksRequest> CreateReadBlocksRequest()
    {
        auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
        return request;
    }

    std::unique_ptr<TEvService::TEvWriteBlocksRequest> CreateWriteBlocksRequest()
    {
        auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();
        return request;
    }

    std::unique_ptr<TEvService::TEvZeroBlocksRequest> CreateZeroBlocksRequest()
    {
        auto request = std::make_unique<TEvService::TEvZeroBlocksRequest>();
        return request;
    }

    template <typename TRequest>
    void SendRequest(std::unique_ptr<TRequest> request)
    {
        auto* ev = new IEventHandle(
            NStorage::MakeUndeliveredHandlerServiceId(),
            Sender,
            request.release());

        Runtime.Send(ev, 0);
    }

#define BLOCKSTORE_DECLARE_METHOD(name, ns)                                    \
    template <typename... Args>                                                \
    void Send##name##Request(Args&&... args)                                   \
    {                                                                          \
        SendRequest(                                                           \
            Create##name##Request(std::forward<Args>(args)...));               \
    }                                                                          \
                                                                               \
    std::unique_ptr<ns::TEv##name##Response> Recv##name##Response()            \
    {                                                                          \
        return RecvResponse<ns::TEv##name##Response>();                        \
    }                                                                          \
                                                                               \
    template <typename... Args>                                                \
    std::unique_ptr<ns::TEv##name##Response> name(Args&&... args)              \
    {                                                                          \
        Send##name##Request(std::forward<Args>(args)...);                      \
        auto response = Recv##name##Response();                                \
        return response;                                                       \
    }                                                                          \
// BLOCKSTORE_DECLARE_METHOD

    BLOCKSTORE_GRPC_STORAGE_SERVICE(BLOCKSTORE_DECLARE_METHOD, TEvService)
    BLOCKSTORE_SERVICE_REQUESTS(BLOCKSTORE_DECLARE_METHOD, TEvService)
    BLOCKSTORE_VOLUME_REQUESTS(BLOCKSTORE_DECLARE_METHOD, TEvVolume)

#undef BLOCKSTORE_DECLARE_METHOD
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TTestActorRuntime> PrepareTestActorRuntime()
{
    auto runtime = std::make_unique<TTestBasicRuntime>();

    auto undeliveredHandler = NStorage::CreateUndeliveredHandler();

    runtime->AddLocalService(
        NStorage::MakeUndeliveredHandlerServiceId(),
        TActorSetupCmd(
            undeliveredHandler.release(),
            TMailboxType::Simple,
            0));

    SetupTabletServices(*runtime);

    return runtime;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TUndeliveredActorTest)
{
    Y_UNIT_TEST(ShouldCancelRequests)
    {
        auto runtime = PrepareTestActorRuntime();
        TUndeliveredClient client(*runtime);

        auto readResponse = client.ReadBlocks();
        UNIT_ASSERT(readResponse->GetError().GetCode() == E_REJECTED);

        auto writeResponse = client.WriteBlocks();
        UNIT_ASSERT(writeResponse->GetError().GetCode() == E_REJECTED);

        auto zeroResponse = client.ZeroBlocks();
        UNIT_ASSERT(zeroResponse->GetError().GetCode() == E_REJECTED);

        auto statResponse = client.StatVolume();
        UNIT_ASSERT(statResponse->GetError().GetCode() == E_REJECTED);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
