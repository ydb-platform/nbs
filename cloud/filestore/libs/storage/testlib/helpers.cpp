#include "helpers.h"

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

std::function<
    bool(NActors::TTestActorRuntimeBase&, TAutoPtr<NActors::IEventHandle>&)>
TTabletRebootTracker::GetEventFilter()
{
    return [this](auto& runtime, auto& event)
    {
        Y_UNUSED(runtime);
        switch (event->GetTypeRewrite()) {
            case NKikimr::TEvTablet::EvBoot: {
                auto* msg = event->template Get<NKikimr::TEvTablet::TEvBoot>();
                Generations.insert(msg->Generation);
                break;
            }
            case NKikimr::TEvTabletPipe::EvClientDestroyed: {
                PipeDestroyed = true;
                break;
            }
        }
        return false;
    };
}

bool TTabletRebootTracker::IsPipeDestroyed() const
{
    return PipeDestroyed;
}

void TTabletRebootTracker::ClearPipeDestroyed()
{
    PipeDestroyed = false;
}

size_t TTabletRebootTracker::GetGenerationCount() const
{
    return Generations.size();
}

}   // namespace NCloud::NFileStore::NStorage
