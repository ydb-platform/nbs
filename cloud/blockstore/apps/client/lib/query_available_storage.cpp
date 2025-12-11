#include "query_available_storage.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TQueryAvailableStorageCommand final: public TCommand
{
private:
    TString StoragePoolName;
    NProto::EStoragePoolKind StoragePoolKind;
    TVector<TString> AgentIds;

public:
    TQueryAvailableStorageCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption(
                "agent-id",
                "agent id (several agents can be added at a time)")
            .RequiredArgument("STR")
            .AppendTo(&AgentIds);

        Opts.AddLongOption("storage-pool-name", "storage pool name")
            .RequiredArgument("STR")
            .StoreResult(&StoragePoolName);

        Opts.AddLongOption("storage-pool-kind", "storage pool kind")
            .RequiredArgument("STR")
            .DefaultValue("local")
            .Handler1T<TString>(
                [this](const auto& s)
                {
                    if (s == "local") {
                        StoragePoolKind = NProto::STORAGE_POOL_KIND_LOCAL;
                    } else if (s == "global") {
                        StoragePoolKind = NProto::STORAGE_POOL_KIND_GLOBAL;
                    } else if (s == "default") {
                        StoragePoolKind = NProto::STORAGE_POOL_KIND_DEFAULT;
                    } else {
                        ythrow yexception()
                            << "unknown storage pool kind: " << s;
                    }
                });
    }

protected:
    bool DoExecute() override
    {
        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading QueryAvailableStorage request");
        auto request =
            std::make_shared<NProto::TQueryAvailableStorageRequest>();
        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            request->SetStoragePoolName(StoragePoolName);
            request->SetStoragePoolKind(StoragePoolKind);
            request->MutableAgentIds()->Assign(
                AgentIds.begin(),
                AgentIds.end());
        }

        STORAGE_DEBUG("Sending QueryAvailableStorage request");
        auto result = WaitFor(ClientEndpoint->QueryAvailableStorage(
            MakeIntrusive<TCallContext>(),
            std::move(request)));

        STORAGE_DEBUG("Received QueryAvailableStorage response");
        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        output << result << Endl;
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewQueryAvailableStorageCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TQueryAvailableStorageCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
