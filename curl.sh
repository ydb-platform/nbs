#!/bin/bash
grpcurl -vv -plaintext -import-path "contrib/libs/googleapis-common-protos/" -import-path "."  -proto cloud/filestore/public/api/grpc/service.proto -d '{"Headers": {"ClientId": "ZnN0", "SessionId": "ZnN0", "SessionSeqNo": "0"}, "FileSystemId": "nfs", "NodeId": "0", "Cookie": "0", "Limit": "100"}' "localhost:9021" NCloud.NFileStore.NProto.TFileStoreService.ReadNodeRefs

