#include "request.h"

#include <cloud/filestore/libs/client/session.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/public/api/protos/data.pb.h>
#include <cloud/filestore/public/api/protos/node.pb.h>

#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/guid.h>
#include <util/generic/size_literals.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/random/random.h>
#include <util/string/builder.h>
#include <util/system/mutex.h>

#include <atomic>

namespace NCloud::NFileStore::NLoadTest {

using namespace NThreading;
using namespace NCloud::NFileStore::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

const ui64 SEGMENT_SIZE = 128;

struct TSegment
{
    ui64 Handle = 0;
    ui64 LastWriteRequestId = 0;
};

static_assert(sizeof(TSegment) <= SEGMENT_SIZE);

bool Compare(
    const TSegment& expected,
    const TSegment& actual,
    TString* message)
{
    TStringBuilder sb;

    if (expected.Handle != actual.Handle) {
        sb << "expected.Handle != actual.Handle: "
            << expected.Handle << " != " << actual.Handle;
    }

    if (expected.LastWriteRequestId != actual.LastWriteRequestId) {
        if (sb.size()) {
            sb << ", ";
        }

        sb << "expected.LastWriteRequestId != actual.LastWriteRequestId: "
            << expected.LastWriteRequestId
            << " != " << actual.LastWriteRequestId;
    }

    *message = sb;

    return sb.empty();
}

struct TSegments: TVector<TSegment>, TAtomicRefCount<TSegments>
{
};

using TSegmentsPtr = TIntrusivePtr<TSegments>;

////////////////////////////////////////////////////////////////////////////////

struct THandleInfo
{
    TString Name;
    ui64 Handle = 0;
    ui64 Size = 0;
    TSegmentsPtr Segments;
    ui64 LastSlot = 0;

    THandleInfo() = default;

    THandleInfo(
            TString name,
            ui64 handle,
            ui64 size,
            TSegmentsPtr segments) noexcept
        : Name(std::move(name))
        , Handle(handle)
        , Size(size)
        , Segments(std::move(segments))
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TDataRequestGenerator final
    : public IRequestGenerator
    , public std::enable_shared_from_this<TDataRequestGenerator>
{
private:
    static constexpr ui32 DefaultBlockSize = 4_KB;

    const NProto::TDataLoadSpec Spec;
    const TString FileSystemId;
    const NProto::THeaders Headers;

    TLog Log;

    ISessionPtr Session;

    TVector<std::pair<ui64, NProto::EAction>> Actions;
    ui64 TotalRate = 0;

    TMutex StateLock;
    ui64 LastWriteRequestId = 0;

    TVector<THandleInfo> IncompleteHandleInfos;
    TVector<THandleInfo> HandleInfos;

    ui64 ReadBytes = DefaultBlockSize;
    ui64 WriteBytes = DefaultBlockSize;
    double AppendProbability = 1;
    ui64 BlockSize = DefaultBlockSize;
    ui64 InitialFileSize = 0;

    std::atomic<ui64> LastRequestId = 0;

public:
    TDataRequestGenerator(
            NProto::TDataLoadSpec spec,
            ILoggingServicePtr logging,
            ISessionPtr session,
            TString filesystemId,
            NProto::THeaders headers)
        : Spec(std::move(spec))
        , FileSystemId(std::move(filesystemId))
        , Headers(std::move(headers))
        , Session(std::move(session))
    {
        Log = logging->CreateLog(Headers.GetClientId());

        if (auto size = Spec.GetBlockSize()) {
            BlockSize = size;
            Y_ENSURE(BlockSize % DefaultBlockSize == 0);
        }
        if (auto bytes = Spec.GetReadBytes()) {
            ReadBytes = bytes;
        }
        if (auto bytes = Spec.GetWriteBytes()) {
            WriteBytes = bytes;
        }
        if (auto p = Spec.GetAppendPercentage()) {
            AppendProbability = p / 100.;
        }

        InitialFileSize = Spec.GetInitialFileSize();

        if (Spec.GetValidationEnabled()) {
            Y_ENSURE(
                InitialFileSize % SEGMENT_SIZE == 0,
                Sprintf(
                    "InitialFileSize (%lu) %% SEGMENT_SIZE (%lu) != 0",
                    InitialFileSize,
                    SEGMENT_SIZE
                )
            );

            Y_ENSURE(
                WriteBytes % SEGMENT_SIZE == 0,
                Sprintf(
                    "WriteBytes (%lu) %% SEGMENT_SIZE (%lu) != 0",
                    WriteBytes,
                    SEGMENT_SIZE
                )
            );

            Y_ENSURE(
                ReadBytes % SEGMENT_SIZE == 0,
                Sprintf(
                    "ReadBytes (%lu) %% SEGMENT_SIZE (%lu) != 0",
                    ReadBytes,
                    SEGMENT_SIZE
                )
            );
        }

        for (const auto& action: Spec.GetActions()) {
            Y_ENSURE(action.GetRate() > 0, "please specify positive action rate");

            TotalRate += action.GetRate();
            Actions.emplace_back(std::make_pair(TotalRate, action.GetAction()));
        }

        Y_ENSURE(!Actions.empty(), "please specify at least one action for the test spec");
    }

    bool HasNextRequest() override
    {
        return true;
    }

    NThreading::TFuture<TCompletedRequest> ExecuteNextRequest() override
    {
        const auto& action = PeekNextAction();
        switch (action) {
        case NProto::ACTION_READ:
            return DoRead();
        case NProto::ACTION_WRITE:
            return DoWrite();
        default:
            Y_ABORT("unexpected action: %u", (ui32)action);
        }
    }

private:
    NProto::EAction PeekNextAction()
    {
        auto number = RandomNumber(TotalRate);
        auto it = LowerBound(
            Actions.begin(),
            Actions.end(),
            number,
            [] (const auto& pair, ui64 b) { return pair.first < b; });

        Y_ABORT_UNLESS(it != Actions.end());
        return it->second;
    }

    TFuture<TCompletedRequest> DoCreateHandle()
    {
        static const int flags = ProtoFlag(NProto::TCreateHandleRequest::E_CREATE)
            | ProtoFlag(NProto::TCreateHandleRequest::E_EXCLUSIVE)
            | ProtoFlag(NProto::TCreateHandleRequest::E_READ)
            | ProtoFlag(NProto::TCreateHandleRequest::E_WRITE);

        auto started = TInstant::Now();
        TGuard<TMutex> guard(StateLock);
        auto name = GenerateNodeName();

        auto request = CreateRequest<NProto::TCreateHandleRequest>();
        request->SetNodeId(RootNodeId);
        request->SetName(name);
        request->SetFlags(flags);

        auto self = weak_from_this();
        return Session->CreateHandle(CreateCallContext(), std::move(request)).Apply(
            [=, name = std::move(name)] (const TFuture<NProto::TCreateHandleResponse>& future) {
                if (auto ptr = self.lock()) {
                    return ptr->HandleCreateHandle(future, name, started);
                }

                return MakeFuture(TCompletedRequest{
                    NProto::ACTION_CREATE_HANDLE,
                    started,
                    MakeError(E_FAIL, "cancelled")});
            });
    }

    TFuture<TCompletedRequest> HandleCreateHandle(
        const TFuture<NProto::TCreateHandleResponse>& future,
        const TString& name,
        TInstant started)
    {
        try {
            auto response = future.GetValue();
            CheckResponse(response);

            auto handle = response.GetHandle();
            auto& infos = InitialFileSize ? IncompleteHandleInfos : HandleInfos;
            with_lock (StateLock) {
                TSegmentsPtr segments;

                if (Spec.GetValidationEnabled()) {
                    segments = new TSegments();
                    segments->resize(Spec.GetInitialFileSize() / SEGMENT_SIZE);
                }

                infos.emplace_back(name, handle, 0, std::move(segments));
            }

            NThreading::TFuture<NProto::TSetNodeAttrResponse> setAttr;
            if (InitialFileSize) {
                static const int flags =
                    ProtoFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);

                auto request = CreateRequest<NProto::TSetNodeAttrRequest>();
                request->SetHandle(handle);
                request->SetNodeId(response.GetNodeAttr().GetId());
                request->SetFlags(flags);
                request->MutableUpdate()->SetSize(InitialFileSize);

                setAttr = Session->SetNodeAttr(CreateCallContext(), std::move(request));
            } else {
                setAttr = NThreading::MakeFuture(NProto::TSetNodeAttrResponse());
            }

            return setAttr.Apply(
                [=, this] (const TFuture<NProto::TSetNodeAttrResponse>& f) {
                    return HandleResizeAfterCreateHandle(f, name, started);
                });
        } catch (const TServiceError& e)  {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR("create handle for %s has failed: %s",
                name.Quote().c_str(),
                FormatError(error).c_str());

            return NThreading::MakeFuture(TCompletedRequest{
                NProto::ACTION_CREATE_HANDLE,
                started,
                error
            });
        }
    }

    TCompletedRequest HandleResizeAfterCreateHandle(
        const TFuture<NProto::TSetNodeAttrResponse>& future,
        const TString& name,
        TInstant started)
    {
        try {
            const auto& response = future.GetValue();
            CheckResponse(response);
            bool handleFound = false;
            with_lock (StateLock) {
                ui32 handleIdx = 0;
                while (handleIdx < IncompleteHandleInfos.size()) {
                    auto& hinfo = IncompleteHandleInfos[handleIdx];
                    if (hinfo.Name == name) {
                        handleFound = true;

                        hinfo.Size = Max(
                            hinfo.Size,
                            response.GetNode().GetSize());

                        STORAGE_INFO(
                            "updated file size for handle %lu and file %s"
                            ", size=%lu",
                            hinfo.Handle,
                            name.Quote().c_str(),
                            hinfo.Size);

                        break;
                    }

                    ++handleIdx;
                }

                if (handleIdx < IncompleteHandleInfos.size()) {
                    DoSwap(
                        IncompleteHandleInfos[handleIdx],
                        IncompleteHandleInfos.back());

                    HandleInfos.push_back(
                        std::move(IncompleteHandleInfos.back()));

                    IncompleteHandleInfos.pop_back();
                }
            }

            if (!handleFound) {
                STORAGE_WARN("handle for file %s not found",
                    name.Quote().c_str());
            }

            return {
                NProto::ACTION_CREATE_HANDLE,
                started,
                response.GetError()
            };
        } catch (const TServiceError& e)  {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR("set node attr handle for %s has failed: %s",
                name.Quote().c_str(),
                FormatError(error).c_str());

            return {
                NProto::ACTION_CREATE_HANDLE,
                started,
                error
            };
        }
    }

    TFuture<TCompletedRequest> DoRead()
    {
        TGuard<TMutex> guard(StateLock);
        if (HandleInfos.empty()) {
            return DoCreateHandle();
        }

        auto handleInfo = GetHandleInfo();
        if (handleInfo.Size < ReadBytes) {
            return DoWrite(handleInfo);
        }

        const auto started = TInstant::Now();
        const ui64 slotOffset = PickSlot(handleInfo, ReadBytes);
        const ui64 byteOffset = slotOffset * ReadBytes;

        auto request = CreateRequest<NProto::TReadDataRequest>();
        request->SetHandle(handleInfo.Handle);
        request->SetOffset(byteOffset);
        request->SetLength(ReadBytes);

        auto self = weak_from_this();
        return Session->ReadData(CreateCallContext(), std::move(request)).Apply(
            [=] (const TFuture<NProto::TReadDataResponse>& future){
                if (auto ptr = self.lock()) {
                    return ptr->HandleRead(
                        future,
                        handleInfo,
                        started,
                        byteOffset
                    );
                }

                return TCompletedRequest{
                    NProto::ACTION_READ,
                    started,
                    MakeError(E_FAIL, "cancelled")};
            });
    }

    TCompletedRequest HandleRead(
        const TFuture<NProto::TReadDataResponse>& future,
        THandleInfo handleInfo,
        TInstant started,
        ui64 byteOffset)
    {
        try {
            auto response = future.GetValue();
            CheckResponse(response);

            const auto& buffer = response.GetBuffer();

            ui64 segmentId = byteOffset / SEGMENT_SIZE;
            for (ui64 offset = 0; offset < ReadBytes; offset += SEGMENT_SIZE) {
                const TSegment* segment =
                    reinterpret_cast<const TSegment*>(buffer.data() + offset);
                if (Spec.GetValidationEnabled()) {
                    Y_ABORT_UNLESS(handleInfo.Segments);

                    auto& segments = *handleInfo.Segments;
                    Y_ABORT_UNLESS(segmentId < segments.size());

                    TString message;
                    if (!Compare(segments[segmentId], *segment, &message)) {
                        throw TServiceError(E_FAIL)
                            << Sprintf("Validation failed: %s", message.c_str());
                    }
                }

                ++segmentId;
            }

            with_lock (StateLock) {
                HandleInfos.emplace_back(std::move(handleInfo));
            }

            return {NProto::ACTION_READ, started, response.GetError()};
        } catch (const TServiceError& e)  {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR("read for %s has failed: %s",
                handleInfo.Name.Quote().c_str(),
                FormatError(error).c_str());

            return {NProto::ACTION_READ, started, error};
        }
    }

    TFuture<TCompletedRequest> DoWrite(THandleInfo handleInfo = {})
    {
        TGuard<TMutex> guard(StateLock);
        if (!handleInfo.Handle) {
            if (HandleInfos.empty()) {
                return DoCreateHandle();
            }

            handleInfo = GetHandleInfo();
        }

        const auto started = TInstant::Now();
        ui64 byteOffset = handleInfo.Size;
        if (RandomNumber<double>() < AppendProbability) {
            handleInfo.Size += WriteBytes;
        } else {
            handleInfo.Size = Max(handleInfo.Size, WriteBytes);
            const ui64 slotOffset = PickSlot(handleInfo, WriteBytes);
            byteOffset = slotOffset * WriteBytes;
        }

        auto request = CreateRequest<NProto::TWriteDataRequest>();
        request->SetHandle(handleInfo.Handle);
        request->SetOffset(byteOffset);

        ++LastWriteRequestId;

        TString buffer(WriteBytes, '\0');
        ui64 segmentId = byteOffset / SEGMENT_SIZE;
        for (ui64 offset = 0; offset < WriteBytes; offset += SEGMENT_SIZE) {
            TSegment* segment =
                reinterpret_cast<TSegment*>(buffer.begin() + offset);
            segment->Handle = handleInfo.Handle;
            segment->LastWriteRequestId = LastWriteRequestId;

            if (Spec.GetValidationEnabled()) {
                Y_ABORT_UNLESS(handleInfo.Segments);

                auto& segments = *handleInfo.Segments;
                if (segments.size() <= segmentId) {
                    segments.emplace_back();
                }

                segments[segmentId] = *segment;
            }

            ++segmentId;
        }
        *request->MutableBuffer() = std::move(buffer);

        auto self = weak_from_this();
        return Session->WriteData(CreateCallContext(), std::move(request)).Apply(
            [=] (const TFuture<NProto::TWriteDataResponse>& future){
                if (auto ptr = self.lock()) {
                    return ptr->HandleWrite(future, handleInfo, started);
                }

                return TCompletedRequest{
                    NProto::ACTION_WRITE,
                    started,
                    MakeError(E_FAIL, "cancelled")};
            });

    }

    TCompletedRequest HandleWrite(
        const TFuture<NProto::TWriteDataResponse>& future,
        THandleInfo handleInfo,
        TInstant started)
    {
        try {
            auto response = future.GetValue();
            CheckResponse(response);

            with_lock (StateLock) {
                HandleInfos.emplace_back(std::move(handleInfo));
            }

            return {NProto::ACTION_WRITE, started, response.GetError()};
        } catch (const TServiceError& e)  {
            auto error = MakeError(e.GetCode(), TString{e.GetMessage()});
            STORAGE_ERROR("write on %s has failed: %s",
                handleInfo.Name.Quote().c_str(),
                FormatError(error).c_str());

            return {NProto::ACTION_WRITE, started, error};
        }
    }

    template <typename T>
    std::shared_ptr<T> CreateRequest()
    {
        auto request = std::make_shared<T>();
        request->SetFileSystemId(FileSystemId);
        request->MutableHeaders()->CopyFrom(Headers);

        return request;
    }

    TString GenerateNodeName()
    {
        return TStringBuilder() << Headers.GetClientId() << ":" << CreateGuidAsString();
    }

    THandleInfo GetHandleInfo()
    {
        Y_ABORT_UNLESS(!HandleInfos.empty());
        ui64 index = RandomNumber(HandleInfos.size());
        std::swap(HandleInfos[index], HandleInfos.back());

        auto handle = HandleInfos.back();
        HandleInfos.pop_back();

        return handle;
    }

    template <typename T>
    void CheckResponse(const T& response)
    {
        if (HasError(response)) {
            throw TServiceError(response.GetError());
        }
    }

    TIntrusivePtr<TCallContext> CreateCallContext()
    {
        return MakeIntrusive<TCallContext>(
            FileSystemId,
            LastRequestId.fetch_add(1, std::memory_order_relaxed));
    }

    ui64 PickSlot(THandleInfo& handleInfo, ui64 reqBytes)
    {
        const ui64 slotCount = handleInfo.Size / reqBytes;
        Y_ABORT_UNLESS(slotCount);
        if (Spec.GetSequential()) {
            return handleInfo.LastSlot = (handleInfo.LastSlot + 1) % slotCount;
        }

        return RandomNumber(slotCount);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRequestGeneratorPtr CreateDataRequestGenerator(
    NProto::TDataLoadSpec spec,
    ILoggingServicePtr logging,
    ISessionPtr session,
    TString filesystemId,
    NProto::THeaders headers)
{
    return std::make_shared<TDataRequestGenerator>(
        std::move(spec),
        std::move(logging),
        std::move(session),
        std::move(filesystemId),
        std::move(headers));
}

}   // namespace NCloud::NFileStore::NLoadTest
