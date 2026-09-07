package snapshot

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nodes_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage/nodes"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type restoredInode struct {
	srcNodeID uint64
	dstNodeID uint64
}

type hardlinkBatchRestorer struct {
	session      nfs.Session
	nodesStorage nodes_storage.Storage
	batchSize    int
	snapshotID   string
	filesystemID string
	workersCount int
}

func newHardlinkBatchRestorer(
	session nfs.Session,
	nodesStorage nodes_storage.Storage,
	snapshotID string,
	filesystemID string,
	batchSize int,
	workersCount int,
) *hardlinkBatchRestorer {

	return &hardlinkBatchRestorer{
		session:      session,
		nodesStorage: nodesStorage,
		batchSize:    batchSize,
		snapshotID:   snapshotID,
		filesystemID: filesystemID,
		workersCount: workersCount,
	}
}

////////////////////////////////////////////////////////////////////////////////

// Restore returns a zero cookie after restoring the final batch.
func (r *hardlinkBatchRestorer) Restore(
	ctx context.Context,
	cookie nodes_storage.HardLinksCookie,
) (nodes_storage.HardLinksCookie, error) {

	batch, nextCookie, err := r.nodesStorage.ListHardLinks(
		ctx,
		r.snapshotID,
		r.batchSize,
		cookie,
	)
	if err != nil || len(batch) == 0 {
		return nodes_storage.HardLinksCookie{}, err
	}

	hardlinksByNodeID := r.groupHardlinksByNodeID(batch)

	parentMapping, err := r.getParentNodeIDsInDestinationFs(ctx, batch)
	if err != nil {
		return cookie, err
	}

	alreadyCreatedNodeIDsMapping, err := r.getAlreadyCreatedNodes(
		ctx,
		hardlinksByNodeID,
	)
	if err != nil {
		return cookie, err
	}

	newMappings, err := r.restoreNodes(
		ctx,
		hardlinksByNodeID,
		parentMapping,
		alreadyCreatedNodeIDsMapping,
	)
	if err != nil {
		return cookie, err
	}

	if len(newMappings) > 0 {
		err = r.nodesStorage.UpdateRestorationNodeIDMapping(
			ctx,
			r.snapshotID,
			r.filesystemID,
			newMappings,
		)
		if err != nil {
			return cookie, err
		}
	}

	return nextCookie, nil
}

////////////////////////////////////////////////////////////////////////////////

func (r *hardlinkBatchRestorer) getParentNodeIDsInDestinationFs(
	ctx context.Context,
	nodes []nfs.Node,
) (map[uint64]uint64, error) {

	parentNodeIDSet := make(map[uint64]struct{})
	for _, node := range nodes {
		parentNodeIDSet[node.ParentNodeID] = struct{}{}
	}

	srcParentNodeIDs := make([]uint64, 0, len(parentNodeIDSet))
	for id := range parentNodeIDSet {
		srcParentNodeIDs = append(srcParentNodeIDs, id)
	}

	return r.nodesStorage.GetDestinationNodeIDs(
		ctx,
		r.snapshotID,
		r.filesystemID,
		srcParentNodeIDs,
	)
}

func (r *hardlinkBatchRestorer) groupHardlinksByNodeID(
	nodes []nfs.Node,
) map[uint64][]nfs.Node {

	hardlinksByNodeID := make(map[uint64][]nfs.Node)
	for _, node := range nodes {
		hardlinksByNodeID[node.NodeID] = append(
			hardlinksByNodeID[node.NodeID],
			node,
		)
	}

	return hardlinksByNodeID
}

func (r *hardlinkBatchRestorer) getAlreadyCreatedNodes(
	ctx context.Context,
	hardlinksByNodeID map[uint64][]nfs.Node,
) (map[uint64]uint64, error) {

	srcNodeIDs := make([]uint64, 0, len(hardlinksByNodeID))
	for nodeID := range hardlinksByNodeID {
		srcNodeIDs = append(srcNodeIDs, nodeID)
	}

	return r.nodesStorage.GetDestinationNodeIDs(
		ctx,
		r.snapshotID,
		r.filesystemID,
		srcNodeIDs,
	)
}

func (r *hardlinkBatchRestorer) restoreNodes(
	ctx context.Context,
	hardlinksByNodeID map[uint64][]nfs.Node,
	parentMapping map[uint64]uint64,
	alreadyCreatedNodeIDsMapping map[uint64]uint64,
) (map[uint64]uint64, error) {

	for _, nodes := range hardlinksByNodeID {
		for i := range nodes {
			if dstParentNodeID, ok := parentMapping[nodes[i].ParentNodeID]; ok {
				nodes[i].ParentNodeID = dstParentNodeID
			}
		}
	}

	newMappings, err := r.restoreInodes(
		ctx,
		hardlinksByNodeID,
		alreadyCreatedNodeIDsMapping,
	)
	if err != nil {
		return nil, err
	}

	err = r.restoreLinks(
		ctx,
		hardlinksByNodeID,
		alreadyCreatedNodeIDsMapping,
		newMappings,
	)
	if err != nil {
		return nil, err
	}

	return newMappings, nil
}

func (r *hardlinkBatchRestorer) restoreInodes(
	ctx context.Context,
	hardlinksByNodeID map[uint64][]nfs.Node,
	alreadyCreatedNodeIDsMapping map[uint64]uint64,
) (map[uint64]uint64, error) {

	missingInodes := 0
	for srcNodeID := range hardlinksByNodeID {
		if _, ok := alreadyCreatedNodeIDsMapping[srcNodeID]; !ok {
			missingInodes++
		}
	}

	eg, ctx := errgroup.WithContext(ctx)
	// Reserve one additional slot for collecting inode IDs.
	eg.SetLimit(r.workersCount + 1)
	results := make(chan restoredInode)
	newMappings := make(map[uint64]uint64)
	eg.Go(
		func() error {
			for i := 0; i < missingInodes; i++ {
				select {
				case result := <-results:
					newMappings[result.srcNodeID] = result.dstNodeID
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			return nil
		},
	)

	for srcNodeID, nodes := range hardlinksByNodeID {
		if _, ok := alreadyCreatedNodeIDsMapping[srcNodeID]; ok {
			continue
		}

		node := nodes[0]
		eg.Go(
			func() error {
				dstNodeID, err := r.restoreInode(ctx, node)
				if err != nil {
					return err
				}

				select {
				case results <- restoredInode{
					srcNodeID: node.NodeID,
					dstNodeID: dstNodeID,
				}:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		)
	}

	err := eg.Wait()
	if err != nil {
		return nil, err
	}

	return newMappings, nil
}

func (r *hardlinkBatchRestorer) restoreLinks(
	ctx context.Context,
	hardlinksByNodeID map[uint64][]nfs.Node,
	alreadyCreatedNodeIDsMapping map[uint64]uint64,
	newMappings map[uint64]uint64,
) error {

	eg, ctx := errgroup.WithContext(ctx)
	eg.SetLimit(r.workersCount)
	for srcNodeID, nodes := range hardlinksByNodeID {
		dstNodeID, ok := alreadyCreatedNodeIDsMapping[srcNodeID]
		if !ok {
			dstNodeID = newMappings[srcNodeID]
			nodes = nodes[1:]
		}

		for _, node := range nodes {
			node := node
			eg.Go(
				func() error {
					return r.restoreLink(ctx, dstNodeID, node)
				},
			)
		}
	}

	return eg.Wait()
}

func (r *hardlinkBatchRestorer) restoreInode(
	ctx context.Context,
	node nfs.Node,
) (uint64, error) {

	dstNodeID, err := r.session.CreateNodeIdempotent(ctx, node)
	if err != nil {
		return 0, err
	}

	logging.Debug(
		ctx,
		"recovered filesystem hardlink source node from snapshot: "+
			"snapshot_id=%v filesystem_id=%v source_node_id=%v "+
			"node=%v dst_node_id=%v",
		r.snapshotID,
		r.filesystemID,
		node.NodeID,
		node,
		dstNodeID,
	)
	return dstNodeID, nil
}

func (r *hardlinkBatchRestorer) restoreLink(
	ctx context.Context,
	dstNodeID uint64,
	node nfs.Node,
) error {

	srcNodeID := node.NodeID
	node.NodeID = dstNodeID
	node.Type = nfs.NODE_KIND_LINK
	_, err := r.session.CreateNodeIdempotent(ctx, node)
	if err != nil {
		return err
	}

	logging.Debug(
		ctx,
		"recovered filesystem hardlink node from snapshot: "+
			"snapshot_id=%v filesystem_id=%v source_node_id=%v "+
			"node=%v dst_node_id=%v",
		r.snapshotID,
		r.filesystemID,
		srcNodeID,
		node,
		dstNodeID,
	)
	return nil
}
