#include "dataset_output.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/service/request_helpers.h>

#include <library/cpp/json/json_writer.h>

#include <util/generic/serialized_enum.h>
#include <util/generic/yexception.h>
#include <util/string/join.h>
#include <util/string/vector.h>

#include <iomanip>

namespace NCloud::NBlockStore {

namespace {

constexpr ui32 RequestsWindowSize = 1024 * 8;
constexpr ui32 BlockSize = 4096;

struct TRequestData
{
    TString DiskId;
    TInstant EndTimestamp;
    EBlockStoreRequest RequestType;
    TDuration ExecTime;
    TDuration PostponedTime;
    ui32 BlockCount;

    ui32 RequestIntersections = 0;
    ui64 BytesInFlight = 0;
    TVector<size_t> Indexes;

    TRequestData() = default;

    TRequestData(
        const TString& diskId,
        const TInstant& endTimestamp,
        EBlockStoreRequest requestType,
        const TDuration& execTime,
        const TDuration& postponedTime,
        const ui32 blockCount)
        : DiskId(diskId)
        , EndTimestamp(endTimestamp)
        , RequestType(requestType)
        , ExecTime(execTime)
        , PostponedTime(postponedTime)
        , BlockCount(blockCount)
    {
        Y_ABORT_UNLESS(ExecTime >= TDuration::Zero());
    }
    explicit TRequestData(TWriteRequest writeRequest)
        : DiskId(std::move(writeRequest.DiskId))
        , EndTimestamp(writeRequest.Timestamp)
        , RequestType(EBlockStoreRequest::WriteBlocks)
        , ExecTime(writeRequest.Duration - writeRequest.Postponed)
        , PostponedTime(writeRequest.Postponed)
        , BlockCount(writeRequest.BlockCount)
    {
        Y_ABORT_UNLESS(ExecTime >= TDuration::Zero());
    }
    ~TRequestData() = default;

    TRequestData& operator=(const TRequestData& other) = default;
    TRequestData& operator=(TRequestData&& other) noexcept = default;
    TRequestData(const TRequestData& other) = default;
    TRequestData(TRequestData&& other) noexcept = default;

    TDuration GetDuration() const
    {
        return ExecTime + PostponedTime;
    }

    TInstant GetStartTimestamp() const
    {
        return EndTimestamp - GetDuration();
    }

    TInstant GetRealStartTimestamp() const
    {
        return EndTimestamp - ExecTime;
    }
};

}   // namespace

///////////////////////////////////////////////////////////////////////////////

class TDatasetOutput::TImpl
{
    TFile File;
    TFileOutput Output;

    TInstant Start;
    TRingBuffer<TRequestData> RequestsWindow;
    // TRingBuffer<TWriteRequest> RequestsWindow;

public:
    explicit TImpl(const TString& filename);
    ~TImpl();

    void ProcessRequests(const NProto::TProfileLogRecord& record);

    void ProcessWriteRequest(TWriteRequest writeRequest);
};

TDatasetOutput::TImpl::TImpl(const TString& filename)
    : File(filename, EOpenModeFlag::CreateAlways | EOpenModeFlag::WrOnly)
    , Output(File)
    , RequestsWindow(RequestsWindowSize)
{}

TDatasetOutput::TImpl::~TImpl() = default;

void TDatasetOutput::TImpl::ProcessRequests(const NProto::TProfileLogRecord& record)
{
    if (record.RequestsSize() == 0) {
        return;
    }

    TVector<TRequestData> currRequests;
    currRequests.reserve(record.RequestsSize());

    if (Start == TInstant::Zero()) {
        Start = TInstant::MicroSeconds(
                    record.GetRequests(0).GetTimestampMcs() -
                    record.GetRequests(0).GetDurationMcs() +
                    record.GetRequests(0).GetPostponedTimeMcs()) +
                TDuration::Seconds(1);
    }

    for (const auto& request: record.GetRequests()) {
        auto requestType =
            static_cast<EBlockStoreRequest>(request.GetRequestType());
        if (!IsReadWriteRequest(requestType)) {
            continue;
        }

        const ui32 blockCount = request.GetRanges().empty()
                                    ? request.GetBlockCount()
                                    : request.GetRanges(0).GetBlockCount();

        auto currRequest = TRequestData(
            record.GetDiskId(),
            TInstant::MicroSeconds(request.GetTimestampMcs()),
            requestType,
            TDuration::MicroSeconds(
                request.GetDurationMcs() - request.GetPostponedTimeMcs()),
            TDuration::MicroSeconds(request.GetPostponedTimeMcs()),
            blockCount);

        Cerr << "currRequest.EndTimestamp = " << currRequest.EndTimestamp.MicroSeconds() << Endl;

        if (RequestsWindow.IsFull()) {
            for (size_t j = 1; j < RequestsWindowSize; ++j) {
                auto& nextRequest = const_cast<TRequestData&>(RequestsWindow.Back(j));
                Y_ABORT_UNLESS(nextRequest.EndTimestamp <= currRequest.EndTimestamp);
                if (nextRequest.EndTimestamp <= currRequest.GetRealStartTimestamp()) {
                    break;
                }
                currRequest.RequestIntersections++;
                nextRequest.RequestIntersections++;

                const ui32 byteCount = nextRequest.BlockCount * BlockSize;
                currRequest.BytesInFlight += byteCount;
                nextRequest.BytesInFlight += byteCount;
            }
        }

        auto droppedRequest = RequestsWindow.PushBack(std::move(currRequest));
        if (!droppedRequest || droppedRequest->EndTimestamp <= Start) {
            continue;
        }

        Y_ABORT_UNLESS(!droppedRequest->DiskId.empty());

        Output
            << droppedRequest->DiskId << " "
            << GetBlockStoreRequestName(droppedRequest->RequestType) << " "
            << droppedRequest->EndTimestamp.MicroSeconds() << " "
            << "exec=" << droppedRequest->ExecTime << " "
            << "postponed=" << droppedRequest->PostponedTime << " "
            << "size=" << droppedRequest->BlockCount * BlockSize << " "
            << "intersections=" << droppedRequest->RequestIntersections << " "
            << "bytes=" << droppedRequest->BytesInFlight << " "
            // << "indexes=["
            // << JoinStrings(request.Indexes.begin(), request.Indexes.end(), ", ")
            // << "]"
            << "\n";

        // currRequests.emplace_back(
        //     record.GetDiskId(),
        //     TInstant::MicroSeconds(request.GetTimestampMcs()),
        //     requestType,
        //     TDuration::MicroSeconds(
        //         request.GetDurationMcs() - request.GetPostponedTimeMcs()),
        //     TDuration::MicroSeconds(request.GetPostponedTimeMcs()),
        //     blockCount);
    }

    // if (currRequests.empty()) {
    //     return;
    // }

    // Cerr << "requests.size() = " << requests.size() << Endl;

    // Sort(
    //     requests.begin(),
    //     requests.end(),
    //     [&](const auto& a, const auto& b)
    //     { return a.EndTimestamp > b.EndTimestamp; });

    // for (size_t i = 0; i < currRequests.size(); ++i) {
    //     auto& request = currRequests[i];
    //     for (size_t j = 1; i >= j && i - j >= 0; ++j) {
    //         auto& nextRequest = currRequests[i - j];
    //         Y_ABORT_UNLESS(nextRequest.EndTimestamp <= request.EndTimestamp);
    //         if (nextRequest.EndTimestamp <= request.GetRealStartTimestamp()) {
    //             break;
    //         }
    //         request.RequestIntersections++;
    //         nextRequest.RequestIntersections++;

    //         const ui32 byteCount = nextRequest.BlockCount * BlockSize;
    //         request.BytesInFlight += byteCount;
    //         nextRequest.BytesInFlight += byteCount;
    //     }


    //     // for (size_t j = 1; j + i < requests.size(); ++j) {
    //     //     auto& nextRequest = requests[i + j];
    //     //     if (nextRequest.EndTimestamp <= request.GetRealStartTimestamp()) {
    //     //         break;
    //     //     }
    //     //     request.RequestIntersections++;
    //     //     nextRequest.RequestIntersections++;
    //     //     // request.Indexes.push_back(i + j);
    //     //     // nextRequest.Indexes.push_back(i);

    //     //     const ui32 byteCount = nextRequest.BlockCount * BlockSize;
    //     //     request.BytesInFlight += byteCount;
    //     //     nextRequest.BytesInFlight += byteCount;
    //     // }
    // }

    // if (Start == TInstant::Zero()) {
    //     Start = currRequests[0].GetRealStartTimestamp() + TDuration::Seconds(1);
    // }

    // const auto endOfRange = currRequests.back().EndTimestamp + TDuration::Seconds(1);

    // Cerr << "endOfRange = " << endOfRange << Endl;

    // for (size_t i = 0; i < currRequests.size(); ++i) {
    //     auto& request = currRequests[i];
    //     if (request.EndTimestamp <= endOfRange) {
    //         break;
    //     }

    //     Output
    //         << i << " " << request.DiskId << " "
    //         << GetBlockStoreRequestName(request.RequestType) << " "
    //         << request.EndTimestamp.MicroSeconds() << " "
    //         << "exec=" << request.ExecTime << " "
    //         << "postponed=" << request.PostponedTime << " "
    //         << "size=" << request.BlockCount * BlockSize << " "
    //         << "intersections=" << request.RequestIntersections << " "
    //         << "bytes=" << request.BytesInFlight << " "
    //         // << "indexes=["
    //         // << JoinStrings(request.Indexes.begin(), request.Indexes.end(), ", ")
    //         // << "]"
    //         << "\n";
    // }
}

void TDatasetOutput::TImpl::ProcessWriteRequest(TWriteRequest writeRequest)
{
    if (Start == TInstant::Zero()) {
        Start = writeRequest.Timestamp - writeRequest.Duration +
                writeRequest.Postponed + TDuration::Seconds(1);
    }

    auto currRequest = TRequestData(std::move(writeRequest));

    // Cerr << "currRequest.EndTimestamp = " << currRequest.EndTimestamp.MicroSeconds() << "; RequestsWindow.size = " << RequestsWindow.Size() << Endl;

    if (RequestsWindow.IsFull()) {
        for (size_t j = 1; j < RequestsWindowSize; ++j) {
            auto& nextRequest = const_cast<TRequestData&>(RequestsWindow.Back(j));
            Y_ABORT_UNLESS(nextRequest.EndTimestamp <= currRequest.EndTimestamp);
            if (nextRequest.EndTimestamp <= currRequest.GetRealStartTimestamp()) {
                break;
            }
            currRequest.RequestIntersections++;
            nextRequest.RequestIntersections++;

            const ui32 byteCount = nextRequest.BlockCount * BlockSize;
            currRequest.BytesInFlight += byteCount;
            nextRequest.BytesInFlight += byteCount;
        }
    }

    auto droppedRequest = RequestsWindow.PushBack(std::move(currRequest));
    if (!droppedRequest || droppedRequest->EndTimestamp <= Start) {
        return;
    }

    Y_ABORT_UNLESS(!droppedRequest->DiskId.empty());

    NJson::TJsonValue jsonResult;
    jsonResult["DiskId"] = droppedRequest->DiskId;
    jsonResult["RequestType"] = GetBlockStoreRequestName(droppedRequest->RequestType);
    jsonResult["EndTimestamp"] = droppedRequest->EndTimestamp.MicroSeconds();
    jsonResult["ExecTime"] = droppedRequest->ExecTime.MicroSeconds();
    jsonResult["PostponedTime"] = droppedRequest->PostponedTime.MicroSeconds();
    jsonResult["ByteSize"] = droppedRequest->BlockCount * BlockSize;
    jsonResult["Intersections"] = droppedRequest->RequestIntersections;
    jsonResult["BytesInFlight"] = droppedRequest->BytesInFlight;

    auto jsonStr = NJson::WriteJson(jsonResult, false, false, false);
    Output << jsonStr << "\n";

    // NJson::TJsonWriter jsonWriter(Output);

    // Output
    //     << droppedRequest->DiskId << " "
    //     << GetBlockStoreRequestName(droppedRequest->RequestType) << " "
    //     << droppedRequest->EndTimestamp.MicroSeconds() << " "
    //     << "exec=" << droppedRequest->ExecTime.MicroSeconds() << " "
    //     << "postponed=" << droppedRequest->PostponedTime.MicroSeconds() << " "
    //     << "size=" << droppedRequest->BlockCount * BlockSize << " "
    //     << "intersections=" << droppedRequest->RequestIntersections << " "
    //     << "bytes=" << droppedRequest->BytesInFlight
    //     << " "
    //     // << "indexes=["
    //     // << JoinStrings(request.Indexes.begin(), request.Indexes.end(), ", ")
    //     // << "]"
    //     << "\n";
}

TDatasetOutput::TDatasetOutput(const TString& filename)
    : Impl(std::make_unique<TImpl>(filename))
{}

TDatasetOutput::~TDatasetOutput() = default;

void TDatasetOutput::ProcessRequests(const NProto::TProfileLogRecord& record)
{
    Impl->ProcessRequests(record);
}

void TDatasetOutput::ProcessWriteRequest(TWriteRequest writeRequest) {
    Impl->ProcessWriteRequest(std::move(writeRequest));
}

}   // namespace NCloud::NBlockStore
