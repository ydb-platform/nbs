#include "disk_agent_private.h"

TString ToString(
    NCloud::NBlockStore::NStorage::TEvDiskAgentPrivate::
        TControlPlaneRequestNumber controlPlaneRequestNumber)
{
    return Sprintf(
        "Generation: %lu, RequestNumber: %lu",
        controlPlaneRequestNumber.Generation,
        controlPlaneRequestNumber.RequestNumber);
}
