#include <cloud/filestore/libs/vfs_fuse/protos/queue_entry.pb.h>

#include <util/generic/queue.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class THandleOpsQueue
{
private:
     TQueue<NProto::TQueueEntry> Requests;

public:
     void AddDestroyRequest(ui64 nodeId, ui64 handle);
     const NProto::TQueueEntry& Front();
     void Pop();
     ui64 Size() const;
     bool Empty() const;
};

}   // namespace NCloud::NFileStore::NFuse
