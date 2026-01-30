#pragma once

#include "cached_write_data_request.h"
#include "pending_write_data_request.h"
#include "persistent_storage.h"
#include "sequence_id_generator.h"

#include <cloud/filestore/public/api/protos/data.pb.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/function_ref.h>
#include <util/generic/intrlist.h>

namespace NCloud::NFileStore::NFuse::NWriteBackCache {

////////////////////////////////////////////////////////////////////////////////

class TPersistentRequestStorage
{
private:
    ISequenceIdGenerator& SequenceIdGenerator;
    IPersistentStorage& PersistentStorage;

    TIntrusiveList<TPendingWriteDataRequest> PendingRequests;
    TIntrusiveList<TCachedWriteDataRequest> UnflushedRequests;
    TIntrusiveList<TCachedWriteDataRequest> FlushedRequests;

public:
    using TAddRequestResult = std::variant<
        std::unique_ptr<TPendingWriteDataRequest>,
        std::unique_ptr<TCachedWriteDataRequest>>;

    using TCachedRequestVisitor =
        TFunctionRef<void(std::unique_ptr<TCachedWriteDataRequest> request)>;

    TPersistentRequestStorage(
        ISequenceIdGenerator& sequenceIdGenerator,
        IPersistentStorage& persistentStorage);

    // Read state from the persistent storage
    bool Init(const TCachedRequestVisitor& visitor);

    bool HasPendingRequests() const;
    bool HasUnflushedRequests() const;

    ui64 GetMinUnflushedSequenceId() const;
    ui64 GetMaxUnflushedCachedSequenceId() const;

    TAddRequestResult AddRequest(
        std::shared_ptr<NProto::TWriteDataRequest> request);

    std::unique_ptr<TCachedWriteDataRequest> TryProcessPendingRequest();

    void Remove(std::unique_ptr<TPendingWriteDataRequest> request);

    void SetFlushed(TCachedWriteDataRequest* request);
    void Evict(std::unique_ptr<TCachedWriteDataRequest> request);
};

}   // namespace NCloud::NFileStore::NFuse::NWriteBackCache
