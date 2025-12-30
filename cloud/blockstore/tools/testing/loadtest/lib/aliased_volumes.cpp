#include "aliased_volumes.h"

#include "client_factory.h"
#include "helpers.h"

#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <util/generic/algorithm.h>
#include <util/generic/yexception.h>

namespace NCloud::NBlockStore::NLoadTest {

////////////////////////////////////////////////////////////////////////////////

void TAliasedVolumes::RegisterAlias(TString volumeName, TString alias)
{
    with_lock (AliasedVolumesLock) {
        AliasedVolumes.push_back({
            std::move(volumeName), std::move(alias)
        });
    }
}

TString TAliasedVolumes::ResolveAlias(TString volumeName) const
{
    if (volumeName.StartsWith("@")) {
        with_lock (AliasedVolumesLock) {
            auto iter = FindIf(
                AliasedVolumes,
                [volumeName] (const auto& v) {
                    return volumeName == v.Alias;
                });
            if (iter == AliasedVolumes.end()) {
                throw yexception()
                    << "Unknown volume alias: " << volumeName;
            }

            volumeName = iter->Name;
        }
    }

    return volumeName;
}

bool TAliasedVolumes::IsAliased(const TString& diskId) const
{
    with_lock (AliasedVolumesLock) {
        return FindIf(AliasedVolumes, [diskId] (const auto& v) {
            return diskId == v.Name;
        }) != AliasedVolumes.end();
    }
}

void TAliasedVolumes::DestroyAliasedVolumesUnsafe(IClientFactory& clientFactory)
{
    ForEach(AliasedVolumes.rbegin(), AliasedVolumes.rend(),
        [this, &clientFactory] (const auto& volume) {
            auto request = std::make_shared<NProto::TDestroyVolumeRequest>();
            request->SetDiskId(volume.Name);

            const auto requestId = GetRequestId(*request);

            auto client =
                clientFactory.CreateClient({}, "destroy-aliased-volumes");
            client->Start();

            STORAGE_INFO("Destroy volume: " << volume.Name);
            WaitForCompletion(
                "DestroyVolume",
                client->DestroyVolume(
                    MakeIntrusive<TCallContext>(requestId),
                    std::move(request)),
                {});

            client->Stop();
        }
    );
}

}   // namespace NCloud::NBlockStore::NLoadTest
