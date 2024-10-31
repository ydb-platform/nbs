#include "dump.h"

#include "request_printer.h"

#include <cloud/filestore/libs/diagnostics/events/profile_events.ev.pb.h>
#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/service/request.h>
#include <cloud/filestore/libs/storage/tablet/model/profile_log_events.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/string/builder.h>
#include <util/string/printf.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

TVector<ui32> GetItemOrder(const NProto::TProfileLogRecord& record)
{
    TVector<ui32> order;
    order.resize(record.RequestsSize());
    std::iota(order.begin(), order.end(), 0);

    Sort(order.begin(), order.end(), [&] (const auto& i, const auto& j) {
        return record.GetRequests(i).GetTimestampMcs()
            < record.GetRequests(j).GetTimestampMcs();
    });

    return order;
}

TString RequestName(const ui32 requestType)
{
    TString name;
    if (requestType < static_cast<ui32>(EFileStoreRequest::MAX)) {
        name = GetFileStoreRequestName(
            static_cast<EFileStoreRequest>(requestType));
    } else if (
        requestType > static_cast<ui32>(NFuse::EFileStoreFuseRequest::MIN) &&
        requestType < static_cast<ui32>(NFuse::EFileStoreFuseRequest::MAX))
    {
        name = GetFileStoreFuseRequestName(
            static_cast<NFuse::EFileStoreFuseRequest>(requestType));
    } else if (
        requestType > static_cast<ui32>(NStorage::EFileStoreSystemRequest::MIN) &&
        requestType < static_cast<ui32>(NStorage::EFileStoreSystemRequest::MAX))
    {
        name = GetFileStoreSystemRequestName(
            static_cast<NStorage::EFileStoreSystemRequest>(requestType));
    }

    // XXX
    if (name.empty() || name.StartsWith("Unknown")) {
        name = Sprintf("Unknown-%u", requestType);
    }

    return name;
}

TVector<TRequestTypeInfo> GetRequestTypes()
{
    TVector<TRequestTypeInfo> ans;
    ans.reserve(2 * FileStoreRequestCount);
    for (ui32 i = 0; i < static_cast<ui32>(EFileStoreRequest::MAX); ++i) {
        ans.emplace_back(i, RequestName(i));
    }
    for (ui32 i = static_cast<ui32>(NFuse::EFileStoreFuseRequest::MIN) + 1;
         i < static_cast<ui32>(NFuse::EFileStoreFuseRequest::MAX);
         ++i)
    {
        ans.emplace_back(i, RequestName(i));
    }
    for (ui32 i = static_cast<ui32>(NStorage::EFileStoreSystemRequest::MIN) + 1;
         i < static_cast<ui32>(NStorage::EFileStoreSystemRequest::MAX);
         ++i)
    {
        ans.emplace_back(i, RequestName(i));
    }
    return ans;
}

void DumpRequest(
    const NProto::TProfileLogRecord& record,
    int i,
    IOutputStream* out)
{
    const auto& r = record.GetRequests(i);

    static THashMap<ui32, IRequestPrinterPtr> printers;
    auto printerIt = printers.find(r.GetRequestType());
    if (printerIt == printers.end()) {
        const auto requestType = r.GetRequestType();
        printerIt =
            printers.emplace(requestType, CreateRequestPrinter(requestType)).first;
    }

    (*out) << TInstant::MicroSeconds(r.GetTimestampMcs())
        << "\t" << record.GetFileSystemId()
        << "\t" << RequestName(r.GetRequestType())
        << "\t" << TDuration::MicroSeconds(r.GetDurationMcs())
        << "\t" << FormatResultCode(r.GetErrorCode())
        << "\t" << printerIt->second->DumpInfo(r) << "\n";
}

}   // namespace NCloud::NFileStore
