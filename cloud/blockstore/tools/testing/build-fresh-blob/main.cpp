#include <cloud/blockstore/libs/storage/partition_common/model/fresh_blob_test.h>
#include <cloud/blockstore/libs/storage/partition2/model/fresh_blob_test.h>

#include <util/system/file.h>


int main(int argc, char** argv)
{
    Y_UNUSED(argc);
    Y_UNUSED(argv);

    {
        using namespace NCloud::NBlockStore::NStorage;

        const auto buffers = GetBuffers(4096);
        const auto blockRanges = GetBlockRanges();
        const auto holders = GetHolders(buffers);

        // Zero timestamp keeps the canonical blob in the original layout.
        const auto blob = BuildWriteFreshBlocksBlobContent(
            blockRanges,
            holders,
            TInstant::Zero());

        TFile file(
            "fresh_write.blob",
            EOpenModeFlag::CreateAlways | EOpenModeFlag::RdWr
        );
        file.Write(blob.data(), blob.size());
    }

    {
        using namespace NCloud::NBlockStore::NStorage;

        const auto blob = BuildZeroFreshBlocksBlobContent(
            ZeroFreshBlocksRange,
            TInstant::Zero());

        TFile file(
            "fresh_zero.blob",
            EOpenModeFlag::CreateAlways | EOpenModeFlag::RdWr
        );
        file.Write(blob.data(), blob.size());
    }

    {
        using namespace NCloud::NBlockStore::NStorage::NPartition2;

        const auto buffers = GetBuffers(4096);
        const auto blockRanges = GetBlockRanges();
        const auto holders = GetHolders(buffers);

        const auto blob = BuildFreshBlobContent(
            blockRanges,
            holders,
            FirstRequestDeletionId
        );

        TFile file(
            "fresh_v2.blob",
            EOpenModeFlag::CreateAlways | EOpenModeFlag::RdWr
        );
        file.Write(blob.data(), blob.size());
    }
}
