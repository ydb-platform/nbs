#include "disk_agent_private.h"

#include <util/stream/output.h>

Y_DECLARE_OUT_SPEC(
    ,
    NCloud::NBlockStore::NStorage::TEvDiskAgentPrivate::
        TControlPlaneRequestNumber,
    o,
    v)
{
    o << "[Generation: " << v.Generation
      << ", RequestNumber: " << v.RequestNumber << "]";
}
