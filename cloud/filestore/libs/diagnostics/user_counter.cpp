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
constexpr TStringBuf FILESTORE_INDEX_OPS         = "filestore.index_ops";
constexpr TStringBuf FILESTORE_INDEX_LATENCY     = "filestore.index_latency";
constexpr TStringBuf FILESTORE_INDEX_ERRORS      = "filestore.index_errors";
constexpr TStringBuf FILESTORE_INDEX_CUMULATIVE_TIME = "filestore.index_cumulative_time";

////////////////////////////////////////////////////////////////////////////////

struct TUserSumCounterWrapper
    : public IUserCounter
{
    TVector<TIntrusivePtr<NMonitoring::TCounterForPtr>> Counters;
    NMonitoring::EMetricType Type = NMonitoring::EMetricType::UNKNOWN;

    void GetType(NMonitoring::IMetricConsumer* consumer) const override
    {
        consumer->OnMetricBegin(Type);
    }

    void GetValue(
        TInstant time,
        NMonitoring::IMetricConsumer* consumer) const override
    {
        int64_t sum = 0;

        for (const auto& counter: Counters) {
            sum += counter->Val();
        }

        consumer->OnInt64(time, sum);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TBucketDescr
{
    TBucketBound Bound;
    TString Name;
};

static constexpr size_t BUCKETS_COUNT = 25;

using TBuckets = std::array<TBucketDescr, BUCKETS_COUNT>;

template <typename THistogramType>
TBuckets MakeBuckets(auto convertBound)
{
    static_assert(BUCKETS_COUNT == THistogramType::BUCKETS_COUNT, "");

    TBuckets result;
    const auto names = THistogramType::MakeNames();
    for (size_t i = 0; i < names.size(); ++i) {
        result[i].Bound = convertBound(THistogramType::Buckets[i]);
        result[i].Name = names[i];
    }
    return result;
}

const TBuckets MS_BUCKETS = MakeBuckets<TRequestMsTimeBuckets>(
    [](double data) {return data;});
const TBuckets US_BUCKETS = MakeBuckets<TRequestUsTimeBuckets>(
    [](double data) {return data == std::numeric_limits<double>::max()
        ? data : data / 1000.;});

////////////////////////////////////////////////////////////////////////////////

struct TUserHistogramWrapper
    : public IUserCounter
{
    static constexpr size_t IGNORE_BUCKETS_COUNT = 10;

    TIntrusivePtr<TDynamicCounters> Counter;
    TIntrusivePtr<TExplicitHistogramSnapshot> Histogram;
    const TBuckets& Buckets;
    EMetricType Type = EMetricType::UNKNOWN;

    TUserHistogramWrapper(const TBuckets& buckets)
        : Histogram(TExplicitHistogramSnapshot::New(
            buckets.size() - IGNORE_BUCKETS_COUNT))
        , Buckets(buckets)
    {
        for (size_t i = IGNORE_BUCKETS_COUNT; i < Buckets.size(); ++i) {
            (*Histogram)[i - IGNORE_BUCKETS_COUNT].first = Buckets[i].Bound;
        }
    }

    void Clear() const
    {
        for (size_t i = IGNORE_BUCKETS_COUNT; i < Buckets.size(); ++i) {
            (*Histogram)[i - IGNORE_BUCKETS_COUNT].second = 0;
        }
    }

    void GetType(NMonitoring::IMetricConsumer* consumer) const override
    {
        consumer->OnMetricBegin(Type);
    }

    void GetValue(
        TInstant time,
        NMonitoring::IMetricConsumer* consumer) const override
    {
        if (!Counter) {
            return;
        }

        Clear();

        for (ui32 i = 0; i < IGNORE_BUCKETS_COUNT; ++i) {
            if (auto countSub = Counter->GetCounter(Buckets[i].Name)) {
                (*Histogram)[0].second += countSub->Val();
            }
        }

        for (ui32 i = IGNORE_BUCKETS_COUNT; i < Buckets.size(); ++i) {
            if (auto countSub = Counter->GetCounter(Buckets[i].Name)) {
                (*Histogram)[i - IGNORE_BUCKETS_COUNT].second += countSub->Val();
            }
        }

        consumer->OnHistogram(time, Histogram);
    }
};

////////////////////////////////////////////////////////////////////////////////

using TBaseDynamicCounters = std::pair<TDynamicCounterPtr, TString>;

void AddUserMetric(
    IUserCounterSupplier& dsc,
    const TLabels& commonLabels,
    const TVector<TBaseDynamicCounters>& baseCounters,
    TStringBuf newName
)
{
    std::shared_ptr<TUserSumCounterWrapper> wrapper =
        std::make_shared<TUserSumCounterWrapper>();

    for (auto& counter: baseCounters) {
        if (counter.first) {
            if (auto countSub = counter.first->FindCounter(counter.second)) {
                wrapper->Counters.push_back(countSub);
                wrapper->Type = countSub->ForDerivative()
                    ? EMetricType::RATE
                    : EMetricType::GAUGE;
            }
        }
    }

    if (wrapper->Type != NMonitoring::EMetricType::UNKNOWN) {
        dsc.AddUserMetric(
            commonLabels,
            newName,
            TUserCounter(wrapper));
    }
}

auto AddHistogramUserMetric(
    const TBuckets& buckets,
    IUserCounterSupplier& dsc,
    const TLabels& commonLabels,
    const TBaseDynamicCounters& baseCounter,
    TStringBuf newName)
{
    auto wrapper = std::make_shared<TUserHistogramWrapper>(buckets);

    wrapper->Type = EMetricType::HIST_RATE;

    if (baseCounter.first) {
        wrapper->Counter =
            baseCounter.first->FindSubgroup("histogram", baseCounter.second);
    }

    dsc.AddUserMetric(
        commonLabels,
        newName,
        TUserCounter(wrapper));
}

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
        MS_BUCKETS,
        dsc,
        commonLabels,
        { readSub, "Time" },
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
        MS_BUCKETS,
        dsc,
        commonLabels,
        { writeSub, "Time" },
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

            const auto& metricName =
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
                    {{indexSubgroup, "Count"}},
                    FILESTORE_INDEX_OPS);
                AddUserMetric(
                    dsc,
                    labels,
                    { {indexSubgroup, "Time"}},
                    FILESTORE_INDEX_CUMULATIVE_TIME
                );
                AddHistogramUserMetric(
                    MS_BUCKETS,
                    dsc,
                    labels,
                    {indexSubgroup, "Time"},
                    FILESTORE_INDEX_LATENCY);
                AddUserMetric(
                    dsc,
                    labels,
                    { {indexSubgroup, "Errors/Fatal"}},
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

}   // NCloud::NFileStore::NUserCounter
