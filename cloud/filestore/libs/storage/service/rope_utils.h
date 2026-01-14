#include <cloud/filestore/libs/service/request.h>

#include <contrib/ydb/library/actors/util/rope.h>

#include <google/protobuf/message.h>

namespace NCloud::NFileStore::NStorage {

TRope CreateRope(
    const ::google::protobuf::RepeatedPtrField<
        ::NCloud::NFileStore::NProto::TIovec>& iovecs);

TRope CreateRope(TString& buffer);

}   // namespace NCloud::NFileStore::NStorage
