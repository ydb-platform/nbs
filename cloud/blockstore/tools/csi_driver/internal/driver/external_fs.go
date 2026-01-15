package driver

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

type ExternalFsConfig struct {
	Id                  string   `json:"fs_id"`
	Type                string   `json:"fs_type"`
	SizeGb              uint64   `json:"fs_size_gb"`
	CloudId             string   `json:"fs_cloud_id"`
	FolderId            string   `json:"fs_folder_id"`
	MountCmd            string   `json:"fs_mount_cmd"`
	MountArgs           []string `json:"fs_mount_args"`
	UmountCmd           string   `json:"fs_umount_cmd"`
	UmountArgs          []string `json:"fs_umount_args"`
	MountExpirationTime string   `json:"fs_mount_expiration_time"`
}

type ExternalFsOverrideMap map[string]ExternalFsConfig

func LoadExternalFsOverrides(filePath string) (ExternalFsOverrideMap, error) {
	if filePath == "" {
		return make(ExternalFsOverrideMap), nil
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	var fsOverrides []ExternalFsConfig
	if err := json.Unmarshal(data, &fsOverrides); err != nil {
		return nil, fmt.Errorf("unmarshalling JSON: %w", err)
	}

	overrideMap := make(ExternalFsOverrideMap)
	for _, fsOverride := range fsOverrides {
		overrideMap[fsOverride.Id] = fsOverride
	}

	log.Printf("ExternalFsOverrideMap: %+v", overrideMap)

	return overrideMap, nil
}

func (om ExternalFsOverrideMap) GetMountExpirationTimes() map[string]int64 {
	expTimes := make(map[string]int64)

	for fsId := range om {
		mountExpirationTimestamp := om[fsId].MountExpirationTime
		if mountExpirationTimestamp == "" {
			continue
		}

		t, err := time.Parse(time.RFC3339, mountExpirationTimestamp)
		if err != nil {
			continue
		}

		expTimes[fsId] = t.Unix()
	}

	return expTimes
}
