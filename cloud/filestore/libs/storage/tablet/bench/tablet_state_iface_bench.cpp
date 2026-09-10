#include <cloud/filestore/libs/storage/core/model.h>
#include <cloud/filestore/libs/storage/tablet/tablet_state_iface.h>

#include <library/cpp/testing/benchmark/bench.h>

#include <util/generic/singleton.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

using namespace NCloud;
using namespace NCloud::NFileStore;
using namespace NCloud::NFileStore::NStorage;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TNodeRef = INodeIndexTabletDatabase::TNodeRef;

constexpr size_t NodeRefCount = 1'000'000;
constexpr ui32 ShardCount = 2500;

const TString MainFileSystemId = "longlongfilesystem-somerandomabcabca";

struct TEncodedNodeRefs
{
    TVector<TNodeRef> Refs;

    TEncodedNodeRefs()
    {
        Refs.resize(NodeRefCount);

        for (size_t i = 0; i < Refs.size(); ++i) {
            auto& ref = Refs[i];
            ref.ShardNodeName = TGUID::Create().AsGuidString();
            ref.ShardId = TStringBuilder()
                << MainFileSystemId
                << ShardNumPrefix
                << i % ShardCount + 1;

            Y_ABORT_UNLESS(ref.TryToEncodeShardId(MainFileSystemId));
        }
    }
};

const TEncodedNodeRefs* GetEncodedNodeRefs()
{
    return Singleton<TEncodedNodeRefs>();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

// TryToDecodeShardId mutates its argument. Copy an encoded ref on every
// iteration so that every call exercises the decoding path. Use the copy-only
// benchmark below as the baseline when calculating the decoder's cost.
Y_CPU_BENCHMARK(TNodeRef_CopyAndTryToDecodeShardId_1MWorkingSet, iface)
{
    const auto& refs = GetEncodedNodeRefs()->Refs;

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        auto ref = refs[i % refs.size()];
        Y_ABORT_UNLESS(ref.TryToDecodeShardId(MainFileSystemId));
        NBench::DoNotOptimize(ref.ShardId);
        NBench::DoNotOptimize(ref.ShardNodeName);
    }
}

Y_CPU_BENCHMARK(TNodeRef_CopyEncoded_1MWorkingSet, iface)
{
    const auto& refs = GetEncodedNodeRefs()->Refs;

    for (size_t i = 0; i < iface.Iterations(); ++i) {
        auto ref = refs[i % refs.size()];
        NBench::DoNotOptimize(ref.ShardId);
        NBench::DoNotOptimize(ref.ShardNodeName);
    }
}
