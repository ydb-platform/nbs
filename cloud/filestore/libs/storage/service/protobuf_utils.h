#include <cloud/filestore/libs/service/request.h>

#include <google/protobuf/message.h>

namespace NCloud::NFileStore {

bool ParseReadDataResponse(
    google::protobuf::io::CodedInputStream& input,
    NProto::TReadDataResponse& response,
    const ::google::protobuf::RepeatedPtrField<
        NProto::TIovec>& iovecs);

}   // namespace NCloud::NFileStore::NStorage
