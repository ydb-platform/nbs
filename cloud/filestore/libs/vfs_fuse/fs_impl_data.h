#pragma once

#include "fs.h"

#include <google/protobuf/repeated_field.h>

struct iovec;

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

// Builds the iovec list for a zero-copy read request from the fuse output
// buffers obtained via fuse_out_buf.
NProto::TError FillReadDataIovecs(
    fuse_req_t req,
    ui64 length,
    google::protobuf::RepeatedPtrField<NProto::TIovec>* iovecs);

// Builds the iovec list for a zero-copy read request from the raw fuse output
// data buffer array (header buffer already excluded by the caller). Each
// segment is capped to the remaining requested length. Returns an error if the
// buffers cannot cover the requested length.
NProto::TError FillReadDataIovecs(
    const struct iovec* iov,
    int count,
    ui64 length,
    google::protobuf::RepeatedPtrField<NProto::TIovec>* iovecs);

// Builds the iovec list for a zero-copy write request from the fuse input
// buffers. Zero-size buffers are skipped. Unlike the read helper, no total
// length validation is performed.
void FillWriteDataIovecs(
    const fuse_bufvec* bufv,
    google::protobuf::RepeatedPtrField<NProto::TIovec>* iovecs);

}   // namespace NCloud::NFileStore::NFuse
