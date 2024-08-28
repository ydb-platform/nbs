#include <cloud/filestore/libs/vfs_fuse/protos/queue_entry.pb.h>

#include <util/generic/queue.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

class TCreateDestroyQueue
{
private:
     TQueue<NProto::TQueueEntry> Requests;

public:
     void AddDestroyRequest(ui64 nodeId, ui64 handle);
     NProto::TQueueEntry GetNext();
     void Remove();
     ui64 Size();
     bool Empty() const;

};

}   // namespace NCloud::NFileStore::NFuse
