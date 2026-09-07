#include "null_storage_group.h"

#include <silk/fibers/fiber.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TNullStorageGroup final: public IStorageGroup
{
private:
    const IDelayPolicyPtr DelayPolicy;

public:
    explicit TNullStorageGroup(IDelayPolicyPtr delayPolicy)
        : DelayPolicy(std::move(delayPolicy))
    {}

    NCloud::NProto::TError AcquireDevices() override
    {
        Wait();
        return {};
    }

    NCloud::NProto::TError ReleaseDevices() override
    {
        Wait();
        return {};
    }

    NCloud::NProto::TError WriteLogRecord(
        NCloud::NProto::TDeviceRequestHeaders headers,
        TVector<TPageGroup> pageGroups,
        ui64 lsn) override
    {
        Y_UNUSED(headers, pageGroups, lsn);

        Wait();
        return {};
    }

    NCloud::NProto::TError ReadPages(
        NCloud::NProto::TDeviceRequestHeaders headers,
        const TVector<TPageGroupRef>& pageGroupRefs,
        TVector<TPageGroup>* pageGroups) override
    {
        Y_UNUSED(headers);

        Wait();

        //
        // Nothing is ever stored, so every read observes zero-filled
        // pages - the same thing a real group returns for untouched
        // storage.
        //

        pageGroups->clear();
        for (const auto& ref: pageGroupRefs) {
            auto& pg = pageGroups->emplace_back();
            pg.FirstPageNo = ref.FirstPageNo;
            for (ui64 i = 0; i < ref.PageCount; ++i) {
                pg.Content.emplace_back().Fill(0, ref.PageSize);
            }
        }

        return {};
    }

private:
    void Wait()
    {
        const TDuration delay = DelayPolicy->NextDelay();
        if (delay) {
            silk::FiberScheduler::sleep(delay.NanoSeconds());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TNullStorageGroupFactory final: public IStorageGroupFactory
{
private:
    const IDelayPolicyPtr DelayPolicy;

public:
    explicit TNullStorageGroupFactory(IDelayPolicyPtr delayPolicy)
        : DelayPolicy(std::move(delayPolicy))
    {}

    IStorageGroupPtr MakeStorageGroup(
        const NProtoPrivate::TPersistentFastShardConfig& config) override
    {
        Y_UNUSED(config);

        return std::make_shared<TNullStorageGroup>(DelayPolicy);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IStorageGroupFactoryPtr CreateNullStorageGroupFactory(
    IDelayPolicyPtr delayPolicy)
{
    return std::make_shared<TNullStorageGroupFactory>(std::move(delayPolicy));
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
