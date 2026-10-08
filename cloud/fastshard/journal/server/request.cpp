#include "request.h"

#include <util/string/builder.h>

namespace NCloud::NJournalled {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest>
void OutHeaders(IOutputStream& out, const TRequest& request)
{
    out << "client: " << request.GetHeaders().GetClientId().Quote();
}

template <typename TRequest>
void OutDeviceUUIDs(IOutputStream& out, const TRequest& request)
{
    out << ", devices: [";
    for (size_t i = 0; i < request.DeviceUUIDsSize(); ++i) {
        out << (i ? ", " : "") << request.GetDeviceUUIDs(i).Quote();
    }
    out << "]";
}

template <typename TRequest>
void OutDeviceUUID(IOutputStream& out, const TRequest& request)
{
    out << ", device: " << request.GetDeviceUUID().Quote();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TString DescribeRequest(const NProto::TAcquireDevicesRequest& request)
{
    TStringBuilder out;
    OutHeaders(out.Out, request);
    OutDeviceUUIDs(out.Out, request);
    out << ", generation: " << request.GetGeneration();
    return out;
}

TString DescribeRequest(const NProto::TReleaseDevicesRequest& request)
{
    TStringBuilder out;
    OutHeaders(out.Out, request);
    OutDeviceUUIDs(out.Out, request);
    return out;
}

TString DescribeRequest(const NProto::TFormatDeviceRequest& request)
{
    TStringBuilder out;
    OutHeaders(out.Out, request);
    OutDeviceUUID(out.Out, request);
    out << ", whole device: " << request.GetWholeDevice();
    return out;
}

TString DescribeRequest(const NProto::TReadPagesRequest& request)
{
    TStringBuilder out;
    OutHeaders(out.Out, request);
    OutDeviceUUID(out.Out, request);
    out << ", pages: [";
    for (size_t i = 0; i < request.PageGroupRefsSize(); ++i) {
        const auto& ref = request.GetPageGroupRefs(i);
        out << (i ? ", " : "") << ref.GetFirstPageNo() << "x"
            << ref.GetPageCount();
    }
    out << "]";
    return out;
}

TString DescribeRequest(const NProto::TWriteLogRecordRequest& request)
{
    TStringBuilder out;
    OutHeaders(out.Out, request);
    OutDeviceUUID(out.Out, request);
    out << ", lsn: " << request.GetLogSequenceNumber()
        << ", prev lsn: " << request.GetPrevLogSequenceNumber() << ", pages: [";
    for (size_t i = 0; i < request.PageGroupsSize(); ++i) {
        const auto& group = request.GetPageGroups(i);
        out << (i ? ", " : "") << group.GetFirstPageNo() << "x"
            << group.ContentSize();
    }
    out << "]";
    return out;
}

TString DescribeRequest(const NProto::TReadJournalTailRequest& request)
{
    TStringBuilder out;
    OutHeaders(out.Out, request);
    OutDeviceUUID(out.Out, request);
    out << ", after lsn: " << request.GetAfterLogSequenceNumber()
        << ", max records: " << request.GetMaxRecordCount();
    return out;
}

TString DescribeRequest(const NProto::TAdvanceLsnLowWatermarkRequest& request)
{
    TStringBuilder out;
    OutHeaders(out.Out, request);
    OutDeviceUUID(out.Out, request);
    out << ", lsn low watermark: " << request.GetLsnLowWatermark();
    return out;
}

}   // namespace NCloud::NJournalled
