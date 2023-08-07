#pragma once

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <util/generic/yexception.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TString GetEntityId(const NProto::TUserNotification& notification);
TString GetEventName(const NProto::TUserNotification& notification);

}   // namespace NCloud::NBlockStore::NStorage
