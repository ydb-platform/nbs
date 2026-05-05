#include <cloud/filestore/libs/service/request.h>

#include <google/protobuf/message.h>

#include <contrib/ydb/library/actors/core/event_pb.h>

namespace NCloud::NFileStore {

NCloud::NProto::TError ParseReadDataResponse(
    NActors::TEventSerializedData& buffer,
    NProto::TReadDataResponse& response,
    const ::google::protobuf::RepeatedPtrField<NProto::TIovec>& iovecs);

}   // namespace NCloud::NFileStore
