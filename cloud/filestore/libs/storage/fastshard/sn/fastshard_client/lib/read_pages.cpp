#include "read_pages.h"

namespace NCloud::NFileStore::NStorage::NFastShard::NClient {

using namespace NLastGetopt;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TReadPagesCommand final: public TCommand
{
private:
    TString DeviceUUID;
    ui64 FirstPageNo = 0;
    ui64 PageCount = 0;
    ui32 PageSize = 4096;

public:
    explicit TReadPagesCommand(IStorageNodePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("device-uuid", "device to read from")
            .RequiredArgument("STR")
            .StoreResult(&DeviceUUID);

        Opts.AddLongOption("first-page-no", "number of the first page to read")
            .RequiredArgument("NUM")
            .StoreResult(&FirstPageNo);

        Opts.AddLongOption("page-count", "number of consecutive pages to read")
            .RequiredArgument("NUM")
            .StoreResult(&PageCount);

        Opts.AddLongOption("page-size", "logical page size in bytes")
            .RequiredArgument("NUM")
            .DefaultValue(PageSize)
            .StoreResult(&PageSize);
    }

protected:
    void CheckOpts() const override
    {
        if (Proto) {
            return;
        }
        if (!DeviceUUID) {
            ythrow TUsageException() << "--device-uuid is required";
        }
        if (!PageCount) {
            ythrow TUsageException() << "--page-count is required";
        }
    }

    bool DoExecute() override
    {
        NCloud::NProto::TReadPagesRequest request;
        if (Proto) {
            ParseFromTextFormat(GetInputStream(), request);
        } else {
            request.SetDeviceUUID(DeviceUUID);
            auto* ref = request.AddPageGroupRefs();
            ref->SetFirstPageNo(FirstPageNo);
            ref->SetPageCount(PageCount);
            ref->SetPageSize(PageSize);
        }
        PrepareHeaders(*request.MutableHeaders());

        auto response = Client->ReadPages(std::move(request));
        if (!HandleResponse(response)) {
            return false;
        }

        if (!Proto) {
            // raw page contents, in request order
            auto& out = GetOutputStream();
            for (const auto& group: response.GetPageGroups()) {
                for (const auto& page: group.GetContent()) {
                    out.Write(page.data(), page.size());
                }
            }
        }
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewReadPagesCommand(IStorageNodePtr client)
{
    return std::make_shared<TReadPagesCommand>(std::move(client));
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard::NClient
