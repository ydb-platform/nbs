package main

import ()

////////////////////////////////////////////////////////////////////////////////

type Device struct {
	DeviceUUID   string `json:"DeviceUUID"`
	State        string `json:"State"`
	StateMessage string `json:"StateMessage"`
}

type Agent struct {
	AgentID string   `json:"AgentId"`
	State   string   `json:"State"`
	Devices []Device `json:"Devices"`
}

type Disk struct {
	DiskID                 string   `json:"DiskId"`
	DeviceUUIDs            []string `json:"DeviceUUIDs"`
	State                  string   `json:"State"`
	FolderID               string   `json:"FolderId"`
	MasterDiskID           string   `json:"MasterDiskId"`
	DeviceReplacementUUIDs []string `json:"DeviceReplacementUUIDs"`
}

type Backup struct {
	Agents []Agent `json:"Agents"`
	Disks  []Disk  `json:"Disks"`
}

type DiskRegistryStateBackup struct {
	Backup Backup `json:"Backup"`
}

////////////////////////////////////////////////////////////////////////////////

type Target struct {
	DiskID      string   `json:"DiskId"`
	DeviceUUIDs []string `json:"DeviceUUIDs"`
	AgentID     string   `json:"AgentId"`
}

func findTestTargets(
	state *DiskRegistryStateBackup,
	folderIDs []string,
) ([]Target, []Target, error) {
	var targets []Target
	var possibleTargets []Target
	var alternativeTargets []Target
	targetDiskIDs := make(map[string]bool)

	replicatingDeviceUUIDs := make(map[string]bool)
	replicatingMirroredDiskIDs := make(map[string]bool)
	deviceUUID2Disk := make(map[string]Disk)
	for _, disk := range state.Backup.Disks {
		for _, deviceUUID := range disk.DeviceUUIDs {
			deviceUUID2Disk[deviceUUID] = disk
		}

		for _, deviceUUID := range disk.DeviceReplacementUUIDs {
			replicatingDeviceUUIDs[deviceUUID] = true
		}

		if len(disk.DeviceReplacementUUIDs) > 0 {
			replicatingMirroredDiskIDs[disk.DiskID] = true
		}
	}

	for _, agent := range state.Backup.Agents {
		mirroredDiskID2DeviceUUIDs := make(map[string]*[]string)
		haveOtherDisks := false
		haveUnsuitableDevices := false
		haveReplicatingDisks := false

		for _, device := range agent.Devices {
			if replicatingDeviceUUIDs[device.DeviceUUID] {
				haveUnsuitableDevices = true
				continue
			}

			if len(device.State) > 0 && device.State != "DEVICE_STATE_ONLINE" {
				haveUnsuitableDevices = true
				continue
			}

			disk, found := deviceUUID2Disk[device.DeviceUUID]

			if found {
				if len(disk.MasterDiskID) != 0 {
					if replicatingMirroredDiskIDs[disk.MasterDiskID] {
						haveReplicatingDisks = true
						continue
					}

					deviceUUIDs, found2 :=
						mirroredDiskID2DeviceUUIDs[disk.MasterDiskID]
					if found2 {
						*deviceUUIDs = append(*deviceUUIDs, device.DeviceUUID)
					} else {
						mirroredDiskID2DeviceUUIDs[disk.MasterDiskID] =
							&[]string{device.DeviceUUID}
					}
				} else {
					haveOtherDisks = true
				}
			}
		}

		if len(mirroredDiskID2DeviceUUIDs) == 0 {
			continue
		}

		if haveOtherDisks || haveUnsuitableDevices || haveReplicatingDisks {
			for diskID, deviceUUIDs := range mirroredDiskID2DeviceUUIDs {
				possibleTargets = append(possibleTargets, Target{
					DiskID:      diskID,
					DeviceUUIDs: *deviceUUIDs,
					AgentID:     agent.AgentID,
				})
			}
		} else {
			for diskID := range mirroredDiskID2DeviceUUIDs {
				target := Target{
					DiskID:  diskID,
					AgentID: agent.AgentID,
				}

				if targetDiskIDs[diskID] {
					// we already have a target with this diskID
					alternativeTargets = append(alternativeTargets, target)

					continue
				}

				targetDiskIDs[diskID] = true

				targets = append(targets, target)
			}
		}
	}

	for _, target := range possibleTargets {
		if targetDiskIDs[target.DiskID] {
			// we already have a target with this diskID
			alternativeTargets = append(alternativeTargets, target)

			continue
		}

		targetDiskIDs[target.DiskID] = true

		targets = append(targets, target)
	}

	return targets, alternativeTargets, nil
}
