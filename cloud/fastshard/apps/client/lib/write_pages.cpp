#include "write_pages.h"

#include <util/generic/yexception.h>

#include <memory>
#include <utility>

namespace NCloud::NFastShard::NClient {

using namespace NLastGetopt;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TWritePagesCommand final: public TCommand
{
private:
    TString DeviceUUID;
    ui64 FirstPageNo = 0;
    ui32 PageSize = 4096;

public:
    explicit TWritePagesCommand(IStorageNodePtr client)
        : TCommand(std::move(client))
    {
        AddAcquireOption(NProto::ACCESS_READ_WRITE);

        Opts.AddLongOption("device-uuid", "device to write to")
            .RequiredArgument("STR")
            .StoreResult(&DeviceUUID);

        Opts.AddLongOption("first-page-no", "number of the first page to write")
            .RequiredArgument("NUM")
            .StoreResult(&FirstPageNo);

        Opts.AddLongOption("page-size")
            .Help("page size in bytes; the input is split into pages of "
                  "this size")
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
        if (!PageSize) {
            ythrow TUsageException() << "--page-size must be positive";
        }
    }

    bool DoExecute() override
    {
        NCloud::NProto::TWritePagesRequest request;
        if (Proto) {
            ParseFromTextFormat(GetInputStream(), request);
        } else {
            request.SetDeviceUUID(DeviceUUID);
            ReadPageGroup(FirstPageNo, PageSize, request.AddPageGroups());
        }
        PrepareHeaders(*request.MutableHeaders());

        auto response = Call(&IStorageNode::WritePages, std::move(request));
        if (!HandleResponse(response)) {
            return false;
        }

        if (!Proto) {
            GetOutputStream() << "OK" << Endl;
        }
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewWritePagesCommand(IStorageNodePtr client)
{
    return std::make_shared<TWritePagesCommand>(std::move(client));
}

}   // namespace NCloud::NFastShard::NClient
