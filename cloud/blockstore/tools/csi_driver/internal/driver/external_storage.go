package driver

import (
	"encoding/json"
	"fmt"
	"os"
)

type ExternalFsConfig struct {
	FsId        string            `json:"fs_id"`
	FsType      string            `json:"fs_type"`
	FsSizeGb    uint64            `json:"fs_size_gb"`
	FsCloudId   string            `json:"fs_cloud_id"`
	FsFolderId  string            `json:"fs_folder_id"`
	FsMountArgs map[string]string `json:"fs_mount_args"`
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
		overrideMap[fsOverride.FsId] = fsOverride
	}

	return overrideMap, nil
}
