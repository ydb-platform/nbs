#include "starter.h"

#include <cloud/blockstore/libs/kms/iface/compute_client.h>
#include <cloud/blockstore/libs/kms/iface/kms_client.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/daemon/app.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/vhost-client/vhost-buffered-client.h>

#include <cloud/contrib/vhost/virtio/virtio_blk_spec.h>

#include <util/system/getpid.h>

namespace NCloud::NBlockStore::NFuzzing {

using namespace NCloud::NBlockStore::NServer;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 BLOCKS_COUNT = 100000000;
constexpr ui64 BLOCKS_SIZE = 4096;

bool ReadData(
    void* dst_data,
    size_t dst_size,
    const ui8* src_data,
    size_t src_size,
    size_t& offset)
{
    if ((src_size - offset) < dst_size) {
        return false;
    }
    memcpy(dst_data, src_data + offset, dst_size);
    offset += dst_size;
    return true;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TStarter* TStarter::GetStarter(
    NServer::TBootstrapBase* bootstrap,
    TVector<TString> options)
{
    static std::unique_ptr<TStarter> Impl = nullptr;

    if (!Impl) {
        Y_ABORT_UNLESS(bootstrap);

        ConfigureSignals();

        Impl = std::unique_ptr<TStarter>(new TStarter(*bootstrap));

        std::set_terminate([]{
            TBackTrace bt;
            bt.Capture();
            Cerr << bt.PrintToString() << Endl;
            abort();
        });

        Impl->ParseOptions(std::move(options));
        Impl->Start();

        atexit([]()
        {
            TStarter::GetStarter(nullptr, {})->Stop();
        });
    }

    return Impl.get();
}

TStarter::TStarter(NServer::TBootstrapBase& bootstrap)
    : Bootstrap(bootstrap)
{
    FuzzerLogging = CreateLoggingService("console", TLogSettings{});
    Log = FuzzerLogging->CreateLog("BLOCKSTORE_FUZZER");
}

void TStarter::ParseOptions(TVector<TString> options)
{
    std::vector<char*> args;
    args.reserve(options.size());

    for(auto& arg: options) {
        args.push_back(&arg[0]);
    }

    Bootstrap.ParseOptions(args.size(), args.data());
}

void TStarter::Start()
{
    try {
        Bootstrap.Init();
        Bootstrap.Start();
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        Bootstrap.Stop();
        throw;
    }

    Thread = SystemThreadFactory()->Run([this](){
        AppMain(Bootstrap.GetShouldContinue());
    });

    {
        auto request = std::make_shared<NProto::TCreateVolumeRequest>();
        request->SetDiskId("vol_" + ToString(GetPID()));
        request->SetBlocksCount(BLOCKS_COUNT);
        request->SetBlockSize(BLOCKS_SIZE);
        request->SetStorageMediaKind(::NCloud::NProto::EStorageMediaKind::STORAGE_MEDIA_HDD);

        auto future = Bootstrap.GetBlockStoreService()->CreateVolume(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        auto result = future.GetValueSync();
        if (HasError(result)) {
            Cerr << FormatError(result.GetError()) << Endl;
            std::exit(-1);
        }
    }

    {
        const TString socket_path = "server_socket_" + ToString(GetPID());

        auto request = std::make_shared<NProto::TStartEndpointRequest>();
        request->SetDiskId("vol_" + ToString(GetPID()));
        request->SetIpcType(NProto::IPC_VHOST);
        request->SetClientId("fuzzer");
        request->SetVhostQueuesCount(2); //max queues count
        request->SetUnixSocketPath(socket_path);

        auto future = Bootstrap.GetBlockStoreService()->StartEndpoint(
            MakeIntrusive<TCallContext>(),
            std::move(request));
        auto result = future.GetValueSync();
        if (HasError(result)) {
            Cerr << FormatError(result.GetError()) << Endl;
            std::exit(-1);
        }

        Client = std::make_unique<NVHost::TBufferedClient>(socket_path);
    }
}

void TStarter::Stop()
{
    AppStop();
    Thread->Join();
    try {
        Bootstrap.Stop();
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        throw;
    }
}

TLog& TStarter::GetLogger()
{
    return Log;
}

int TStarter::Run(const ui8* data, size_t size)
{
    size_t offset = 0;

    if ((size - sizeof(virtio_blk_req_hdr)) % Client->GetBufferSize() != 0) {
        return -1;
    }

    if ((size - sizeof(virtio_blk_req_hdr)) / Client->GetBufferSize() == 0) {
        return -1;
    }

    virtio_blk_req_hdr hdr;
    if (!ReadData(&hdr.type,     sizeof(hdr.type),     data, size, offset) ||
        !ReadData(&hdr.reserved, sizeof(hdr.reserved), data, size, offset) ||
        !ReadData(&hdr.sector,   sizeof(hdr.sector),   data, size, offset))
    {
        return -1;
    }

    if (hdr.sector >
        BLOCKS_COUNT * BLOCKS_SIZE / Client->GetBufferSize())
    {
        return -1;
    }

    TVector<TVector<char>> inData;
    TVector<TVector<char>> outData;

    inData.push_back(TVector<char>(sizeof(virtio_blk_req_hdr)));
    memcpy(inData.back().data(), &hdr, sizeof(virtio_blk_req_hdr));

    if (hdr.type != VIRTIO_BLK_T_OUT && hdr.type != VIRTIO_BLK_T_IN) {
        return -1;
    }

    while (size > offset) {
        const size_t bufferSize = (size - offset) > Client->GetBufferSize()
            ? Client->GetBufferSize()
            : (size - offset);

        if (hdr.type == VIRTIO_BLK_T_OUT) {
            inData.push_back(TVector<char>(bufferSize));
            memcpy(inData.back().data(), data + offset, bufferSize);
        } else if (hdr.type == VIRTIO_BLK_T_IN) {
            outData.push_back(TVector<char>(bufferSize));
        }

        offset += bufferSize;
    }
    outData.push_back(TVector<char>(1));
    return Client->Write(inData, outData) ? 0 : -1;
}

}   // namespace NCloud::NBlockStore::NFuzzing
