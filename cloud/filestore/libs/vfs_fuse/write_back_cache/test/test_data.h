#pragma once

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>

class TLog;

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

// Extracts the data carried by a write request's buffer or iovecs.
TString ExtractWriteData(const NProto::TWriteDataRequest& request);

////////////////////////////////////////////////////////////////////////////////

class TTestData
{
private:
    class TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TTestData();
    ~TTestData();

    void EnableLogReads(TString logTag, const TLog& log);
    void EnableLogWrites(TString logTag, const TLog& log);

    // Executes a read request using either the response buffer or its iovecs.
    // For buffer responses, responseBufferOffsetLimit is the exclusive upper
    // bound for a randomly selected BufferOffset that is used for testing
    // purposes. The selected offset reserves that many bytes at the beginning
    // of Buffer. A zero limit disables the offset; the parameter is ignored
    // when the request supplies iovecs.
    NProto::TReadDataResponse Read(
        const NProto::TReadDataRequest& request,
        ui32 responseBufferOffsetLimit = 0) const;

    TString Read(ui64 nodeId, ui64 offset, ui64 length) const;

    // Executes a write request and returns the data extracted from its buffer
    // or iovecs.
    TString Write(const NProto::TWriteDataRequest& request);
    void Write(ui64 nodeId, ui64 offset, TStringBuf data);

    bool Contains(ui64 nodeId) const;

    // Returns all data stored for the requested node.
    TString ReadAll(ui64 nodeId) const;

    // Returns the identifiers of all stored nodes in unspecified order.
    TVector<ui64> GetNodeIds() const;

    // Returns a deterministic serialization of all stored node data. The
    // result is not an atomic snapshot across nodes.
    TString Dump() const;

    // Returns the total number of bytes supplied to successful writes.
    ui64 GetBytesWritten() const;
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
