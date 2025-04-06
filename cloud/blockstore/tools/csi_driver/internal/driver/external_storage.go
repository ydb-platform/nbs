package driver

import (
	"encoding/json"
	"fmt"
	"os"
)

type WekaMountArgs struct {
	ShareName         string   `json:"share_name"`
	SharePath         string   `json:"share_path"`
	AuthToken         string   `json:"auth_token"`
	StorageNodes      []string `json:"storage_nodes"`
	ExtraMountOptions []string `json:"extra_mount_options"`
}

type VastMountArgs struct {
	ShareName         string   `json:"share_name"`
	SharePath         string   `json:"share_path"`
	StorageNodes      []string `json:"storage_nodes"`
	ExtraMountOptions []string `json:"extra_mount_options"`
}

type LocalFilestoreOverride struct {
	FsId          string         `json:"fs_id"`
	FsType        string         `json:"fs_type"`
	FsSizeGb      uint64         `json:"fs_size_gb"`
	FsParentId    string         `json:"fs_parent_id"`
	WekaMountArgs *WekaMountArgs `json:"weka_mount_args"`
	VastMountArgs *VastMountArgs `json:"vast_mount_args"`
}

type LocalFilestoreOverrideMap map[string]LocalFilestoreOverride

func LoadLocalFilestoreOverrides(filePath string) (LocalFilestoreOverrideMap, error) {
	if filePath == "" {
		return make(LocalFilestoreOverrideMap), nil
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("reading file: %w", err)
	}

	var fsOverrides []LocalFilestoreOverride
	if err := json.Unmarshal(data, &fsOverrides); err != nil {
		return nil, fmt.Errorf("unmarshalling JSON: %w", err)
	}

	overrideMap := make(LocalFilestoreOverrideMap)
	for _, fsOverride := range fsOverrides {
		overrideMap[fsOverride.FsId] = fsOverride
	}

	return overrideMap, nil
}
