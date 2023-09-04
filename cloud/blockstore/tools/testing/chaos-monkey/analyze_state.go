package main

import (
	"fmt"
	"log"
	"strings"
)

////////////////////////////////////////////////////////////////////////////////

type Device struct {
	DeviceUUID   string `json:"DeviceUUID"`
	State        string `json:"State"`
	StateMessage string `json:"StateMessage"`
	Rack         string `json:"Rack"`
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
	ReplicaCount           int      `json:"ReplicaCount"`
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

type BriefDeviceInfo struct {
	AgentID      string `json:"AgentId"`
	DeviceUUID   string `json:"DeviceUUID"`
	State        string `json:"State"`
	StateMessage string `json:"StateMessage"`
}

type CompleteDeviceInfo struct {
	DiskID     string
	Device     Device
	Agent      Agent
	DeviceInfo BriefDeviceInfo
}

type DeviceInfoMap map[string]CompleteDeviceInfo

type CellInfo struct {
	Index      int               `json:"Index"`
	GoodCount  int               `json:"GoodCount"`
	BadCount   int               `json:"BadCount"`
	FreshCount int               `json:"FreshCount"`
	State      string            `json:"State"`
	Devices    []BriefDeviceInfo `json:"Devices"`
}

type DeviceByAgentMap map[string][]string
type DeviceByRackMap map[string][]string

type RackInfo struct {
	Rack    string   `json:"Rack"`
	Replica string   `json:"Replica"`
	Devices []string `json:"Devices"`
}

type DiskInfo struct {
	DiskID              string            `json:"DiskId"`
	MirrorCount         int               `json:"MirrorCount"`
	DeviceInMirrorCount int               `json:"DeviceInMirrorCount"`
	DeviceOK            int               `json:"DeviceOK"`
	DeviceMinus1        int               `json:"DeviceMinus1"`
	DeviceMinus2        int               `json:"DeviceMinus2"`
	DeviceBad           int               `json:"DeviceBad"`
	ReplicatingCount    int               `json:"ReplicatingCount"`
	Replicas            []string          `json:"Replicas"`
	Cells               []CellInfo        `json:"Cells"`
	Agents              DeviceByAgentMap  `json:"Agents"`
	Racks               []RackInfo        `json:"Racks"`
	BrokenDevices       []BriefDeviceInfo `json:"BrokenDevices"`
}

type DeviceTargetType int64

const (
	OneDeviceForAllFine  DeviceTargetType = 0
	OneDeviceForMinusOne DeviceTargetType = 1
	TwoDevicesForMirror3 DeviceTargetType = 2
	FreshDevice          DeviceTargetType = 3
)

func (e DeviceTargetType) String() string {
	switch e {
	case OneDeviceForAllFine:
		return "OneDeviceForAllFine"
	case OneDeviceForMinusOne:
		return "OneDeviceForMinusOne"
	case TwoDevicesForMirror3:
		return "TwoDevicesForMirror3"
	case FreshDevice:
		return "FreshDevice"
	default:
		return fmt.Sprintf("%d", int(e))
	}
}

////////////////////////////////////////////////////////////////////////////////

type Target struct {
	DiskID      string   `json:"DiskId"`
	DeviceUUIDs []string `json:"DeviceUUIDs"`
	AgentID     string   `json:"AgentId"`
	Reason      string   `json:"Reason"`
}

func findDiskID(state *DiskRegistryStateBackup, deviceUUID string) string {
	for _, disk := range state.Backup.Disks {
		for _, device := range disk.DeviceUUIDs {
			if deviceUUID == device {
				return disk.DiskID
			}
		}
	}
	return ""
}

func getAllDevices(state *DiskRegistryStateBackup) DeviceInfoMap {
	result := make(DeviceInfoMap)

	for i := range state.Backup.Agents {
		agent := &state.Backup.Agents[i]
		for _, device := range agent.Devices {
			result[device.DeviceUUID] = CompleteDeviceInfo{
				DiskID: findDiskID(state, device.DeviceUUID),
				Agent:  *agent,
				Device: device,
				DeviceInfo: BriefDeviceInfo{
					AgentID:      agent.AgentID,
					DeviceUUID:   device.DeviceUUID,
					State:        device.State,
					StateMessage: device.StateMessage,
				},
			}
		}
	}
	return result
}

func findDiskState(state *DiskRegistryStateBackup, diskID string) *Disk {
	for _, disk := range state.Backup.Disks {
		if disk.DiskID == diskID {
			return &disk
		}
	}
	return nil
}

func fillReplicaDevices(disk *Disk, devices *DeviceInfoMap, diskInfo *DiskInfo) {
	diskInfo.Replicas = append(diskInfo.Replicas, disk.DiskID)
	for indx, deviceUUID := range disk.DeviceUUIDs {
		if len(diskInfo.Cells) <= indx {
			diskInfo.Cells = append(diskInfo.Cells, CellInfo{
				Index: indx,
			})
		}
		cell := &diskInfo.Cells[indx]
		completeDeviceInfo := (*devices)[deviceUUID]
		cell.Devices = append(cell.Devices, completeDeviceInfo.DeviceInfo)
		if len(completeDeviceInfo.Device.State) == 0 {
			cell.GoodCount++
		} else {
			cell.BadCount++
		}
	}
}

func findDevice(diskInfo *DiskInfo, deviceUUID string) (*CellInfo, *BriefDeviceInfo) {
	for i := range diskInfo.Cells {
		cell := &diskInfo.Cells[i]
		for j := range cell.Devices {
			deviceInfo := &cell.Devices[j]
			if deviceInfo.DeviceUUID == deviceUUID {
				return cell, deviceInfo
			}
		}
	}
	return nil, nil
}

func fillDevices(disk *Disk, diskInfo *DiskInfo) {
	for _, deviceUUID := range disk.DeviceReplacementUUIDs {
		mirrorInfo, deviceInfo := findDevice(diskInfo, deviceUUID)
		if deviceInfo == nil {
			log.Fatalf("Not found device %v", deviceUUID)
		}
		if len(deviceInfo.State) != 0 {
			continue
		}
		mirrorInfo.FreshCount++
		mirrorInfo.GoodCount--
		deviceInfo.State = "fresh"
		mirrorInfo.State = "replicating"
	}
}

func addDeviceToRack(racks *[]RackInfo, device *CompleteDeviceInfo) {
	var rack *RackInfo
	for indx := range *racks {
		rackInfo := &(*racks)[indx]
		if rackInfo.Rack != device.Device.Rack {
			continue
		}
		rack = rackInfo
	}
	if rack == nil {
		(*racks) = append(*racks, RackInfo{})
		rack = &(*racks)[len(*racks)-1]
		rack.Rack = device.Device.Rack
		rack.Replica = device.DiskID
	}
	rack.Devices = append(rack.Devices, device.Device.DeviceUUID)
}

func isDeviceFromDisk(diskInfo *DiskInfo, deviceUUID string) bool {
	for _, cell := range diskInfo.Cells {
		for _, device := range cell.Devices {
			if device.DeviceUUID == deviceUUID {
				return true
			}
		}
	}
	return false
}

func describeDisk(state *DiskRegistryStateBackup, diskID string) (*DiskInfo, error) {
	main := findDiskState(state, diskID)
	if main == nil {
		return nil, fmt.Errorf("Disk '%v' not found", diskID)
	}

	allDevices := getAllDevices(state)

	diskInfo := DiskInfo{
		DiskID:      main.DiskID,
		MirrorCount: main.ReplicaCount + 1,
		Agents:      make(DeviceByAgentMap),
	}

	for _, disk := range state.Backup.Disks {
		if disk.MasterDiskID == diskID {
			fillReplicaDevices(&disk, &allDevices, &diskInfo)
		}
	}

	for _, disk := range state.Backup.Disks {
		if disk.DiskID == diskID {
			fillDevices(&disk, &diskInfo)
		}
	}

	diskInfo.DeviceInMirrorCount = len(diskInfo.Cells)

	for _, mirrorInfo := range diskInfo.Cells {
		if mirrorInfo.GoodCount == diskInfo.MirrorCount {
			diskInfo.DeviceOK++
		} else if mirrorInfo.GoodCount == diskInfo.MirrorCount-1 && mirrorInfo.GoodCount != 0 {
			diskInfo.DeviceMinus1++
		} else if mirrorInfo.GoodCount == diskInfo.MirrorCount-2 && mirrorInfo.GoodCount != 0 {
			diskInfo.DeviceMinus2++
		} else if mirrorInfo.GoodCount == 0 {
			diskInfo.DeviceBad++
		}
		if mirrorInfo.FreshCount != 0 {
			diskInfo.ReplicatingCount++
		}
		for _, device := range mirrorInfo.Devices {
			deviceRawInfo := allDevices[device.DeviceUUID]

			diskInfo.Agents[deviceRawInfo.Agent.AgentID] = append(diskInfo.Agents[deviceRawInfo.Agent.AgentID], device.DeviceUUID)
			addDeviceToRack(&diskInfo.Racks, &deviceRawInfo)
		}
	}

	stateMessagePrefix := fmt.Sprintf("MirroredDiskId=%v", diskID)
	for _, deviceInfo := range allDevices {
		if (deviceInfo.Device.State == "DEVICE_STATE_ERROR") &&
			(strings.HasPrefix(deviceInfo.Device.StateMessage, stateMessagePrefix) ||
				isDeviceFromDisk(&diskInfo, deviceInfo.Device.DeviceUUID)) {
			diskInfo.BrokenDevices = append(diskInfo.BrokenDevices, deviceInfo.DeviceInfo)
		}
	}

	return &diskInfo, nil
}

func isDeviceInList(cursedDevices []Target, deviceUUID string) bool {
	for _, target := range cursedDevices {
		for _, device := range target.DeviceUUIDs {
			if device == deviceUUID {
				return true
			}
		}
	}
	return false
}

func findTargetsToCurse(
	diskInfo *DiskInfo,
	targetCount int,
	cursedDevices []Target,
	targetType DeviceTargetType,
) []Target {

	var targets []Target

	devicesToCurse := make(map[string][]string)

	for _, cell := range diskInfo.Cells {
		if targetCount == 0 {
			break
		}

		// Check that no device in the cell will be cursed by previous calls.
		hasCursedDevice := false
		for _, device := range cell.Devices {
			isCursedDevice := isDeviceInList(cursedDevices, device.DeviceUUID)
			if isCursedDevice {
				hasCursedDevice = true
				break
			}
		}
		if hasCursedDevice {
			continue
		}

		// Curse two devices.
		if targetType == TwoDevicesForMirror3 && cell.GoodCount == 3 && targetCount > 1 {
			// curse two devices only when disk is mirror-3 and all devices are online
			for i := 0; i < 2; i++ {
				device := cell.Devices[i]
				devicesToCurse[device.AgentID] = append(
					devicesToCurse[device.AgentID],
					device.DeviceUUID)
				targetCount--
			}
			continue
		}

		if targetType == OneDeviceForAllFine && cell.GoodCount != diskInfo.MirrorCount {
			continue
		}

		if targetType == OneDeviceForMinusOne && (diskInfo.MirrorCount != 3 || cell.GoodCount != 2) {
			// curse second device only when disk is mirror-3
			continue
		}

		if targetType == FreshDevice && cell.FreshCount == 0 {
			continue
		}

		for _, device := range cell.Devices {
			isReadyDevice := len(device.State) == 0
			isFreshDevice := device.State == "fresh"
			canCurse := (targetType == OneDeviceForAllFine && isReadyDevice) ||
				(targetType == OneDeviceForMinusOne && isReadyDevice) ||
				(targetType == FreshDevice && isFreshDevice)

			if canCurse {
				devicesToCurse[device.AgentID] = append(
					devicesToCurse[device.AgentID],
					device.DeviceUUID)
				targetCount--
				break
			}

		}
	}

	for agentID, deviceUUIDs := range devicesToCurse {
		targets = append(targets, Target{
			DiskID:      diskInfo.DiskID,
			AgentID:     agentID,
			DeviceUUIDs: deviceUUIDs,
			Reason:      targetType.String(),
		})
	}

	return targets
}

func findAllTargetsToCurse(
	diskInfo *DiskInfo,
	targetCount int,
	canBreakTwoDevicesInCell bool,
) []Target {

	var result []Target

	if canBreakTwoDevicesInCell {
		result = findTargetsToCurse(diskInfo, targetCount, result, TwoDevicesForMirror3)
	}

	targetCount -= len(result)
	if targetCount > 0 {
		oneForAllFine := findTargetsToCurse(diskInfo, targetCount, result, OneDeviceForAllFine)
		result = append(result, oneForAllFine...)
	}

	targetCount -= len(result)
	if targetCount > 0 && canBreakTwoDevicesInCell {
		minusOneTargets := findTargetsToCurse(diskInfo, targetCount, result, OneDeviceForMinusOne)
		result = append(result, minusOneTargets...)
	}

	targetCount -= len(result)
	if targetCount > 0 {
		freshDeviceTargets := findTargetsToCurse(diskInfo, targetCount, result, FreshDevice)
		result = append(result, freshDeviceTargets...)
	}

	return result
}

func findTargetsToHeal(
	diskInfo *DiskInfo,
	targetCount int,
) []BriefDeviceInfo {

	var devicesToHeal []BriefDeviceInfo

	for _, brokenDevice := range diskInfo.BrokenDevices {
		if targetCount == 0 {
			break
		}
		devicesToHeal = append(
			devicesToHeal, brokenDevice)
		targetCount--
	}

	return devicesToHeal
}
