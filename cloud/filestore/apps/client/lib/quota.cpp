#include "command.h"

namespace NCloud::NFileStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TSetQuotaCommand final
    : public TFileStoreCommand
{
private:
    ui32 QuotaId = 0;
    ui64 MaxBytes = 0;
    ui64 MaxNodes = 0;

public:
    TSetQuotaCommand()
    {
        Opts.AddLongOption("quota-id")
            .Required()
            .RequiredArgument("NUM")
            .Help("creates the quota or replaces both its limits")
            .StoreResult(&QuotaId);

        Opts.AddLongOption("max-bytes")
            .Required()
            .RequiredArgument("NUM")
            .Help("byte limit, 0 means unlimited")
            .StoreResult(&MaxBytes);

        Opts.AddLongOption("max-nodes")
            .Required()
            .RequiredArgument("NUM")
            .Help("node limit, 0 means unlimited")
            .StoreResult(&MaxNodes);
    }

    bool Execute() override
    {
        auto callContext = PrepareCallContext();

        auto request = std::make_shared<NProto::TSetQuotaRequest>();
        request->SetFileSystemId(FileSystemId);
        request->SetQuotaId(QuotaId);
        request->SetMaxBytes(MaxBytes);
        request->SetMaxNodes(MaxNodes);

        auto response = WaitFor(
            Client->SetQuota(std::move(callContext), std::move(request)));

        if (HasError(response)) {
            STORAGE_THROW_SERVICE_ERROR(response.GetError());
        }

        if (JsonOutput) {
            response.PrintJSON(Cout);
        } else {
            Cout << ProtoMessagePrinter.ToString(response.GetQuota()) << Endl;
        }

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDeleteQuotaCommand final
    : public TFileStoreCommand
{
private:
    ui32 QuotaId = 0;

public:
    TDeleteQuotaCommand()
    {
        Opts.AddLongOption("quota-id")
            .Required()
            .RequiredArgument("NUM")
            .StoreResult(&QuotaId);
    }

    bool Execute() override
    {
        auto callContext = PrepareCallContext();

        auto request = std::make_shared<NProto::TDeleteQuotaRequest>();
        request->SetFileSystemId(FileSystemId);
        request->SetQuotaId(QuotaId);

        auto response = WaitFor(
            Client->DeleteQuota(std::move(callContext), std::move(request)));

        if (HasError(response)) {
            STORAGE_THROW_SERVICE_ERROR(response.GetError());
        }

        return true;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TListQuotasCommand final
    : public TFileStoreCommand
{
public:
    TListQuotasCommand() = default;

    bool Execute() override
    {
        auto callContext = PrepareCallContext();

        auto request = std::make_shared<NProto::TListQuotasRequest>();
        request->SetFileSystemId(FileSystemId);

        auto response = WaitFor(
            Client->ListQuotas(std::move(callContext), std::move(request)));

        if (HasError(response)) {
            STORAGE_THROW_SERVICE_ERROR(response.GetError());
        }

        if (JsonOutput) {
            response.PrintJSON(Cout);
            return true;
        }

        for (const auto& quota: response.GetQuotas()) {
            Cout << "Quota: " << ProtoMessagePrinter.ToString(quota) << Endl;
        }
        for (const auto& usage: response.GetUsages()) {
            Cout << "Usage: " << ProtoMessagePrinter.ToString(usage) << Endl;
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewSetQuotaCommand()
{
    return std::make_shared<TSetQuotaCommand>();
}

TCommandPtr NewDeleteQuotaCommand()
{
    return std::make_shared<TDeleteQuotaCommand>();
}

TCommandPtr NewListQuotasCommand()
{
    return std::make_shared<TListQuotasCommand>();
}

}   // namespace NCloud::NFileStore::NClient
