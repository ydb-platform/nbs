#include "user_counter.h"

#include <cloud/storage/core/libs/diagnostics/histogram_types.h>

namespace NCloud::NFileStore::NUserCounter {

using namespace NMonitoring;
using namespace NCloud::NStorage::NUserStats;

namespace {

////////////////////////////////////////////////////////////////////////////////

// Read/Write counters
constexpr TStringBuf FILESTORE_READ_OPS          = "filestore.read_ops";
constexpr TStringBuf FILESTORE_READ_OPS_BURST    = "filestore.read_ops_burst";
constexpr TStringBuf FILESTORE_READ_BYTES        = "filestore.read_bytes";
constexpr TStringBuf FILESTORE_READ_BYTES_BURST  = "filestore.read_bytes_burst";
constexpr TStringBuf FILESTORE_READ_LATENCY      = "filestore.read_latency";
constexpr TStringBuf FILESTORE_READ_ERRORS       = "filestore.read_errors";
constexpr TStringBuf FILESTORE_WRITE_OPS         = "filestore.write_ops";
constexpr TStringBuf FILESTORE_WRITE_OPS_BURST   = "filestore.write_ops_burst";
constexpr TStringBuf FILESTORE_WRITE_BYTES       = "filestore.write_bytes";
constexpr TStringBuf FILESTORE_WRITE_BYTES_BURST = "filestore.write_bytes_burst";
constexpr TStringBuf FILESTORE_WRITE_LATENCY     = "filestore.write_latency";
constexpr TStringBuf FILESTORE_WRITE_ERRORS      = "filestore.write_errors";

// Index operation counters
constexpr TStringBuf FILESTORE_INDEX_OPS             = "filestore.index_ops";
constexpr TStringBuf FILESTORE_INDEX_LATENCY         = "filestore.index_latency";
constexpr TStringBuf FILESTORE_INDEX_ERRORS          = "filestore.index_errors";
constexpr TStringBuf FILESTORE_INDEX_CUMULATIVE_TIME = "filestore.index_cumulative_time";

TLabels MakeFilestoreLabels(
    const TString& cloudId,
    const TString& folderId,
    const TString& filestoreId,
    const TString& instanceId)
{
    return {
        {"service", "compute"},
        {"project", cloudId},
        {"cluster", folderId},
        {"filestore", filestoreId},
        {"instance", instanceId}};
}

const THashMap<TString, TString>& GetIndexOpsNames()
{
    static const THashMap<TString, TString> names = {
        {"AllocateData", "fallocate"},
        {"CreateHandle", "open"},
        {"CreateNode", "createnode"},
        {"DestroyHandle", "release"},
        {"GetNodeAttr", "getattr"},
        {"GetNodeXAttr", "getxattr"},
        {"ListNodeXAttr", "listxattr"},
        {"ListNodes", "readdir"},
        {"RenameNode", "rename"},
        {"SetNodeAttr", "setattr"},
        {"SetNodeXAttr", "setxattr"},
        {"UnlinkNode", "unlink"},
        {"StatFileStore", "statfs"},
        {"ReadLink", "readlink"},
        {"AccessNode", "access"},
        {"RemoveNodeXAttr", "removexattr"},
        {"ReleaseLock", "releaselock"},
        {"AcquireLock", "acquirelock"}};

    return names;
}

TLabels MakeFilestoreLabelsWithRequestName(
    const TString& cloudId,
    const TString& folderId,
    const TString& filestoreId,
    const TString& instanceId,
    const TString& requestName)
{
    auto labels =
        MakeFilestoreLabels(cloudId, folderId, filestoreId, instanceId);
    labels.Add("request", requestName);
    return labels;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void RegisterFilestore(
    IUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& filestoreId,
    const TString& instanceId,
    NMonitoring::TDynamicCounterPtr src)
{
    if (instanceId.empty() || !src) {
        return;
    }

    const auto commonLabels =
        MakeFilestoreLabels(cloudId, folderId, filestoreId, instanceId);

    auto readSub = src->FindSubgroup("request", "ReadData");
    AddUserMetric(
        dsc,
        commonLabels,
        { { readSub, "Count" } },
        FILESTORE_READ_OPS);
    AddUserMetric(
        dsc,
        commonLabels,
        { { readSub, "MaxCount" } },
        FILESTORE_READ_OPS_BURST);
    AddUserMetric(
        dsc,
        commonLabels,
        { { readSub, "RequestBytes" } },
        FILESTORE_READ_BYTES);
    AddUserMetric(
        dsc,
        commonLabels,
        { { readSub, "MaxRequestBytes" } },
        FILESTORE_READ_BYTES_BURST);
    AddUserMetric(
        dsc,
        commonLabels,
        { { readSub, "Errors/Fatal" } },
        FILESTORE_READ_ERRORS);
    AddHistogramUserMetric(
        GetMsBuckets(),
        dsc,
        commonLabels,
        { { readSub, "Time" } },
        FILESTORE_READ_LATENCY);

    auto writeSub = src->FindSubgroup("request", "WriteData");
    AddUserMetric(
        dsc,
        commonLabels,
        { { writeSub, "Count" } },
        FILESTORE_WRITE_OPS);
    AddUserMetric(
        dsc,
        commonLabels,
        { { writeSub, "MaxCount" } },
        FILESTORE_WRITE_OPS_BURST);
    AddUserMetric(
        dsc,
        commonLabels,
        { { writeSub, "RequestBytes" } },
        FILESTORE_WRITE_BYTES);
    AddUserMetric(
        dsc,
        commonLabels,
        { { writeSub, "MaxRequestBytes" } },
        FILESTORE_WRITE_BYTES_BURST);
    AddUserMetric(
        dsc,
        commonLabels,
        { { writeSub, "Errors/Fatal" } },
        FILESTORE_WRITE_ERRORS);
    AddHistogramUserMetric(
        GetMsBuckets(),
        dsc,
        commonLabels,
        { { writeSub, "Time" } },
        FILESTORE_WRITE_LATENCY);

    TVector<TBaseDynamicCounters> indexOpsCounters;
    TVector<TBaseDynamicCounters> indexErrorCounters;

    auto requestSnapshot = src->ReadSnapshot();
    for (auto& request: requestSnapshot) {
        if (request.first.LabelName == "request" &&
            request.first.LabelValue != "ReadData" &&
            request.first.LabelValue != "WriteData")
        {
            const auto indexSubgroup =
                src->FindSubgroup("request", request.first.LabelValue);

            indexOpsCounters.emplace_back(indexSubgroup, "Count");
            indexErrorCounters.emplace_back(indexSubgroup, "Errors/Fatal");

            auto metricName =
                GetIndexOpsNames().find(request.first.LabelValue);
            if (metricName) {
                const auto labels = MakeFilestoreLabelsWithRequestName(
                    cloudId,
                    folderId,
                    filestoreId,
                    instanceId,
                    metricName->second);
                AddUserMetric(
                    dsc,
                    labels,
                    { { indexSubgroup, "Count" } },
                    FILESTORE_INDEX_OPS);
                AddUserMetric(
                    dsc,
                    labels,
                    { { indexSubgroup, "Time" } },
                    FILESTORE_INDEX_CUMULATIVE_TIME
                );
                AddHistogramUserMetric(
                    GetMsBuckets(),
                    dsc,
                    labels,
                    {{ indexSubgroup, "Time" }},
                    FILESTORE_INDEX_LATENCY);
                AddUserMetric(
                    dsc,
                    labels,
                    { { indexSubgroup, "Errors/Fatal" } },
                    FILESTORE_INDEX_ERRORS
                );
            }
        }
    }

    AddUserMetric(
        dsc,
        commonLabels,
        indexOpsCounters,
        FILESTORE_INDEX_OPS);
    AddUserMetric(
        dsc,
        commonLabels,
        indexErrorCounters,
        FILESTORE_INDEX_ERRORS);
}

void UnregisterFilestore(
    IUserCounterSupplier& dsc,
    const TString& cloudId,
    const TString& folderId,
    const TString& filestoreId,
    const TString& instanceId)
{
    const auto commonLabels =
        MakeFilestoreLabels(cloudId, folderId, filestoreId, instanceId);

    dsc.RemoveUserMetric(commonLabels, FILESTORE_READ_OPS);
    dsc.RemoveUserMetric(commonLabels, FILESTORE_READ_OPS_BURST);
    dsc.RemoveUserMetric(commonLabels, FILESTORE_READ_BYTES);
    dsc.RemoveUserMetric(commonLabels, FILESTORE_READ_BYTES_BURST);
    dsc.RemoveUserMetric(commonLabels, FILESTORE_READ_LATENCY);
    dsc.RemoveUserMetric(commonLabels, FILESTORE_READ_ERRORS);

    dsc.RemoveUserMetric(commonLabels, FILESTORE_WRITE_OPS);
    dsc.RemoveUserMetric(commonLabels, FILESTORE_WRITE_OPS_BURST);
    dsc.RemoveUserMetric(commonLabels, FILESTORE_WRITE_BYTES);
    dsc.RemoveUserMetric(commonLabels, FILESTORE_WRITE_BYTES_BURST);
    dsc.RemoveUserMetric(commonLabels, FILESTORE_WRITE_LATENCY);
    dsc.RemoveUserMetric(commonLabels, FILESTORE_WRITE_ERRORS);

    dsc.RemoveUserMetric(commonLabels, FILESTORE_INDEX_OPS);
    dsc.RemoveUserMetric(commonLabels, FILESTORE_INDEX_ERRORS);

    for (const auto& [_, requestName]: GetIndexOpsNames()) {
        const auto labels = MakeFilestoreLabelsWithRequestName(
            cloudId,
            folderId,
            filestoreId,
            instanceId,
            requestName);
        dsc.RemoveUserMetric(labels, FILESTORE_INDEX_OPS);
        dsc.RemoveUserMetric(labels, FILESTORE_INDEX_CUMULATIVE_TIME);
        dsc.RemoveUserMetric(labels, FILESTORE_INDEX_LATENCY);
        dsc.RemoveUserMetric(labels, FILESTORE_INDEX_ERRORS);
    }
}

}  // namespace NCloud::NFileStore::NUserCounter
