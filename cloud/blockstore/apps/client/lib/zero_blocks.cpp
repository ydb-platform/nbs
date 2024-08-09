#include "zero_blocks.h"

#include <cloud/blockstore/libs/client/session.h>
#include <cloud/blockstore/libs/encryption/model/utils.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore::NClient {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TZeroBlocksCommand final
    : public TCommand
{
private:
    TString DiskId;
    ui64 StartIndex = 0;
    ui64 BlocksCount = 0;
    TString MountToken;
    bool ThrottlingDisabled = false;
    bool MountLocal = false;
    NProto::EEncryptionMode EncryptionMode = NProto::NO_ENCRYPTION;
    TString EncryptionKeyPath;
    TString EncryptionKeyHash;

    bool ZeroAll = false;

    ISessionPtr Session;

public:
    TZeroBlocksCommand(IBlockStorePtr client)
        : TCommand(std::move(client))
    {
        Opts.AddLongOption("start-index", "start block index")
            .RequiredArgument("NUM")
            .StoreResult(&StartIndex);

        Opts.AddLongOption("blocks-count", "maximum number of blocks stored in volume")
            .RequiredArgument("NUM")
            .StoreResult(&BlocksCount);

        Opts.AddLongOption("disk-id", "volume identifier")
            .RequiredArgument("STR")
            .StoreResult(&DiskId);

        Opts.AddLongOption("token", "Mount token")
            .RequiredArgument("STR")
            .StoreResult(&MountToken);

        Opts.AddLongOption("throttling-disabled")
            .Help("Use ThrottlingDisabled mount flag")
            .NoArgument()
            .SetFlag(&ThrottlingDisabled);

        Opts.AddLongOption("mount-local")
            .Help("mount the volume locally")
            .NoArgument()
            .SetFlag(&MountLocal);

        Opts.AddLongOption("encryption-mode", "encryption mode [no|aes-xts|test]")
            .RequiredArgument("STR")
            .Handler1T<TString>([this] (const auto& s) {
                EncryptionMode = EncryptionModeFromString(s);
            });

        Opts.AddLongOption("encryption-key-path", "path to file with encryption key")
            .RequiredArgument("STR")
            .StoreResult(&EncryptionKeyPath);

        Opts.AddLongOption("encryption-key-hash", "key hash for snapshot mode")
            .RequiredArgument("STR")
            .StoreResult(&EncryptionKeyHash);

        Opts.AddLongOption("zero-all", "Zero all blocks of volume")
            .NoArgument();
    }

protected:
    bool DoExecute() override
    {
        const auto* zeroAll = ParseResultPtr->FindLongOptParseResult("zero-all");
        if (zeroAll) {
            ZeroAll = true;
        }

        if (!Proto && !CheckOpts()) {
            return false;
        }

        if (Proto && ZeroAll) {
            STORAGE_ERROR("--zero-all option cannot be used along with --proto option");
            return false;
        }

        auto& input = GetInputStream();
        auto& output = GetOutputStream();

        STORAGE_DEBUG("Reading ZeroBlocks request");
        if (Proto) {
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            ParseFromTextFormat(input, *request);
            DiskId = request->GetDiskId();
            StartIndex = request->GetStartIndex();
            BlocksCount = request->GetBlocksCount();
        }

        auto encryptionSpec = CreateEncryptionSpec(
            EncryptionMode,
            EncryptionKeyPath,
            EncryptionKeyHash);

        auto mountResponse = MountVolume(
            DiskId,
            MountToken,
            Session,
            NProto::VOLUME_ACCESS_READ_WRITE,
            MountLocal,
            ThrottlingDisabled,
            encryptionSpec
        );
        if (HasError(mountResponse)) {
            return false;
        }

        if (ZeroAll) {
            StartIndex = 0;
            BlocksCount = mountResponse.GetVolume().GetBlocksCount();
        }

        NProto::TZeroBlocksResponse result;
        try {
            auto request = std::make_shared<NProto::TZeroBlocksRequest>();
            request->SetStartIndex(StartIndex);
            request->SetBlocksCount(BlocksCount);
            PrepareHeaders(*request->MutableHeaders());

            result = WaitFor(Session->ZeroBlocks(
                MakeIntrusive<TCallContext>(),
                std::move(request)));
        } catch(...) {
            STORAGE_ERROR(CurrentExceptionMessage());
            UnmountVolume(*Session);
            return false;
        }

        STORAGE_DEBUG("Received ZeroBlocks response");

        if (!UnmountVolume(*Session)) {
            return false;
        }

        if (Proto) {
            SerializeToTextFormat(result, output);
            return true;
        }

        const auto& error = result.GetError();

        if (FAILED(error.GetCode())) {
            output << FormatError(error) << Endl;
            return false;
        }

        output << "OK" << Endl;
        return true;
    }

private:
    bool CheckOpts() const
    {
        const auto* diskId = ParseResultPtr->FindLongOptParseResult("disk-id");
        if (!diskId) {
            STORAGE_ERROR("Disk id is required");
            return false;
        }

        if (!ZeroAll) {
            const auto* startIndex = ParseResultPtr->FindLongOptParseResult("start-index");
            if (!startIndex) {
                STORAGE_ERROR("Start index is required");
                return false;
            }

            const auto* blocksCount = ParseResultPtr->FindLongOptParseResult("blocks-count");
            if (!blocksCount) {
                STORAGE_ERROR("Blocks count is required");
                return false;
            }
        }

        return true;
    }
};

} // namespace

////////////////////////////////////////////////////////////////////////////////

TCommandPtr NewZeroBlocksCommand(IBlockStorePtr client)
{
    return MakeIntrusive<TZeroBlocksCommand>(std::move(client));
}

}   // namespace NCloud::NBlockStore::NClient
