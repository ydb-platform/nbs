#pragma once

#include "public.h"

#include <cloud/storage/core/protos/media.pb.h>
#include <cloud/storage/core/protos/trace.pb.h>

#include <library/cpp/containers/ring_buffer/ring_buffer.h>
#include <library/cpp/lwtrace/log.h>

#include <util/generic/hash.h>

namespace NLWTrace {
class TManager;
}   // namespace NLWTrace

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct TLWTraceThreshold
{
    TDuration Default;
    TDuration PerSizeUnit;
    THashMap<TString, TDuration> ByRequestType;
};

using TRequestThresholds =
    THashMap<NProto::EStorageMediaKind, TLWTraceThreshold>;

using TProtoRequestThresholds =
    google::protobuf::RepeatedPtrField<NCloud::NProto::TLWTraceThreshold>;

TRequestThresholds ConvertRequestThresholds(
    const TProtoRequestThresholds& value);

void OutRequestThresholds(
    IOutputStream& out,
    const NCloud::TRequestThresholds& value);

struct TEntry
{
    TInstant Ts;
    ui64 Date = 0;
    NLWTrace::TTrackLog TrackLog;
    TString Tag;
};

struct ITraceReader
{
    const TString Id;

    explicit ITraceReader(TString id)
        : Id(std::move(id))
    {}

    virtual ~ITraceReader() = default;

    virtual void Push(TThread::TId tid, const NLWTrace::TTrackLog&) = 0;
    virtual void ForEach(std::function<void(const TEntry&)> fn) = 0;
    virtual void Reset() = 0;
};

struct ITraceReaderWithRingBuffer
    : public ITraceReader
{
    static constexpr size_t DefaultRingBufferSize = 1000;

    TSimpleRingBuffer<TEntry> RingBuffer;

    explicit ITraceReaderWithRingBuffer(TString id)
        : ITraceReader(std::move(id))
        , RingBuffer(DefaultRingBufferSize)
    {}

    void ForEach(std::function<void(const TEntry&)> fn) override
    {
        for (ui64 i = RingBuffer.FirstIndex(); i < RingBuffer.TotalSize(); ++i) {
            fn(RingBuffer[i]);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ITraceReaderPtr CreateTraceLogger(
    TString id,
    ILoggingServicePtr logging,
    TString componentName);

ITraceReaderPtr CreateSlowRequestsFilter(
    TString id,
    ILoggingServicePtr logging,
    TString componentName,
    TRequestThresholds requestThresholds);

NLWTrace::TQuery ProbabilisticQuery(
    const TVector<std::tuple<TString, TString>>& probes,
    ui32 samplingRate);

NLWTrace::TQuery ProbabilisticQuery(
    const TVector<std::tuple<TString, TString>>& probes,
    ui32 samplingRate,
    ui32 shuttleCount);

TDuration GetThresholdByRequestType(
    const NProto::EStorageMediaKind mediaKind,
    const TRequestThresholds& requestThresholds,
    const NLWTrace::TParam* requestTypeParam,
    const NLWTrace::TParam* requestSizeParam);

}   // namespace NCloud
