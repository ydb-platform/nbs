#include "describe_blob.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <contrib/ydb/core/base/logoblob.h>

#include <library/cpp/protobuf/util/pb_io.h>

#include <google/protobuf/util/json_util.h>

#include <util/string/split.h>

namespace NCloud::NBlockStore::NClient {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool ParseBlobId(
    const TString& str,
    NKikimr::TLogoBlobID& blobId,
    TString& errorExplanation)
{
    if (NKikimr::TLogoBlobID::Parse(blobId, str, errorExplanation)) {
        return true;
    }

    TStringBuf raw = str;
    ui64 rawX1 = 0;
    ui64 rawX2 = 0;
    ui64 rawX3 = 0;
    if (!raw.SkipPrefix("[") || !raw.ChopSuffix("]") ||
        !StringSplitter(raw).Split(':').TryCollectInto(&rawX1, &rawX2, &rawX3))
    {
        errorExplanation = "expected [tablet:gen:step:channel:cookie:size:part]"
                           " or [RawX1:RawX2:RawX3]";
        return false;
    }

    blobId = NKikimr::TLogoBlobID(rawX1, rawX2, rawX3);
    return true;
}

////////////////////////////////////////////////////////////////////////////////

class TDescribeBlobCommand final
    : public TCommand
{
private:
    TString DiskId;
    TString BlobId;

public:
    explicit TDescribeBlobCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("disk-id", "volume identifier")
            .RequiredArgument("STR")
            .StoreResult(&DiskId);

        Opts.AddLongOption(
                "blob-id",
                "blob id as [tablet:gen:step:channel:cookie:size:part]"
                " or [RawX1:RawX2:RawX3]")
            .RequiredArgument("STR")
            .StoreResult(&BlobId);
    }

protected:
    bool DoExecute() override
    {
        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        auto request = std::make_shared<NProto::TExecuteActionRequest>();

        if (!Proto && !CheckOpts()) {
            return false;
        }

        if (Proto) {
            ParseFromTextFormat(input, *request);
        } else {
            NKikimr::TLogoBlobID blobId;
            TString errorExplanation;
            if (!ParseBlobId(BlobId, blobId, errorExplanation)) {
                auto error = MakeError(
                    E_ARGUMENT,
                    TStringBuilder()
                        << "invalid blob id: " << BlobId.Quote()
                        << " (" << errorExplanation << ")");
                output << FormatError(error) << Endl;
                return false;
            }

            NPrivateProto::TDescribeBlobRequest requestProto;
            requestProto.SetDiskId(DiskId);

            auto* requestBlobId = requestProto.MutableBlobId();
            const auto* raw = blobId.GetRaw();
            requestBlobId->SetRawX1(raw[0]);
            requestBlobId->SetRawX2(raw[1]);
            requestBlobId->SetRawX3(raw[2]);

            TString input;
            google::protobuf::util::MessageToJsonString(requestProto, &input);
            request->SetAction("describeblob");
            request->SetInput(std::move(input));
        }

        STORAGE_DEBUG("Sending DescribeBlob request");
        const auto requestId = GetRequestId(*request);
        auto result = WaitFor(ClientEndpoint->ExecuteAction(
            MakeIntrusive<TCallContext>(requestId),
            std::move(request)));

        STORAGE_DEBUG("Received DescribeBlob response");

        if (HasError(result)) {
            output << FormatError(result.GetError()) << Endl;
            return false;
        }

        if (Proto) {
            NPrivateProto::TDescribeBlobResponse responseProto;
            google::protobuf::util::JsonParseOptions parseOpts;
            parseOpts.ignore_unknown_fields = true;
            const auto parsed = google::protobuf::util::JsonStringToMessage(
                result.GetOutput(),
                &responseProto,
                parseOpts).ok();

            if (!parsed) {
                auto error = MakeError(
                    E_BADMSG,
                    TStringBuilder() << "failed to parse response json: "
                        << result.GetOutput());
                output << FormatError(error) << Endl;
                return false;
            }

            SerializeToTextFormat(responseProto, output);
            return true;
        }

        output << result.GetOutput() << Endl;
        return true;
    }

private:
    bool CheckOpts() const
    {
        if (!ParseResultPtr->FindLongOptParseResult("disk-id")) {
            STORAGE_ERROR("Disk id is required");
            return false;
        }

        if (!ParseResultPtr->FindLongOptParseResult("blob-id")) {
            STORAGE_ERROR("Blob id is required");
            return false;
        }

        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewDescribeBlobCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TDescribeBlobCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
