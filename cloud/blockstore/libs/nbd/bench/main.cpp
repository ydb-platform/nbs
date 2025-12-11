#include "bootstrap.h"
#include "options.h"

#include <cloud/blockstore/libs/client/client.h>
#include <cloud/blockstore/libs/nbd/client.h>
#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist_test.h>

#include <library/cpp/testing/benchmark/bench.h>

namespace NCloud::NBlockStore::NBD {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void CheckError(TFuture<T> future)
{
    const auto& response = future.GetValueSync();
    if (HasError(response)) {
        auto error = FormatError(response.GetError());
        Y_ABORT("Request failed with error: %s", error.c_str());
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_CPU_BENCHMARK(WriteBlocks, iface)
{
    auto options = std::make_shared<TOptions>();
    options->FiltrationLevel = TLOG_ERR;
    options->StructuredReply = true;

    auto bootstrap = std::make_shared<TBootstrap>(options);
    bootstrap->Init();
    bootstrap->Start();

    auto client = bootstrap->GetClient();

    auto context = MakeIntrusive<TCallContext>();

    auto blocksCount = 42;
    TString block(options->BlockSize, 'a');

    TVector<TString> vec;
    auto sglist = ResizeBlocks(vec, blocksCount, block);

    auto request = std::make_shared<NProto::TWriteBlocksLocalRequest>();
    request->BlocksCount = blocksCount;
    request->BlockSize = options->BlockSize;
    request->Sglist = TGuardedSgList(std::move(sglist));

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        request->SetStartIndex(i);
        CheckError(client->WriteBlocksLocal(context, request));
    }

    bootstrap->Stop();
}

}   // namespace NCloud::NBlockStore::NBD
