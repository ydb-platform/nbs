package nbs

type diskRegistryCheckpointReplica struct {
	CheckpointID string `json:"CheckpointId"`
	SourceDiskID string `json:"SourceDiskId"`
}

type diskRegistryDisk struct {
	DiskID            string                        `json:"DiskId"`
	DeviceUUIDs       []string                      `json:"DeviceUUIDs"`
	CheckpointReplica diskRegistryCheckpointReplica `json:"CheckpointReplica"`
}

type diskRegistryDevice struct {
	DeviceUUID string `json:"DeviceUUID"`
}

type diskRegistryAgent struct {
	Devices []diskRegistryDevice `json:"Devices"`
	AgentID string               `json:"AgentId"`
}

type DiskRegistryBackup struct {
	Disks  []diskRegistryDisk  `json:"Disks"`
	Agents []diskRegistryAgent `json:"Agents"`
}

type diskRegistryState struct {
	Backup DiskRegistryBackup `json:"Backup"`
}

func (b *DiskRegistryBackup) GetDevicesOfShadowDisk(diskID string) []string {
	for _, disk := range b.Disks {
		if disk.CheckpointReplica.SourceDiskID == diskID {
			return disk.DeviceUUIDs
		}
	}
	return nil
}

func (b *DiskRegistryBackup) GetAgentIDByDeviceUUId(deviceUUID string) string {
	for _, agent := range b.Agents {
		for _, device := range agent.Devices {
			if device.DeviceUUID == deviceUUID {
				return agent.AgentID
			}
		}
	}
	return ""
}
