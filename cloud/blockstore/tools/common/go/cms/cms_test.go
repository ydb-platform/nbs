package cms

import (
	"context"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/blockstore/config"
	nbs "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	kikimr "github.com/ydb-platform/nbs/cloud/blockstore/tools/common/go/cms/proto"
	"github.com/ydb-platform/nbs/cloud/nbs_internal/storage/core/tools/common/go/pssh"
	logutil "github.com/ydb-platform/nbs/cloud/storage/core/tools/common/go/log"
	"github.com/ydb-platform/nbs/cloud/storage/core/tools/common/go/ssh"
)

////////////////////////////////////////////////////////////////////////////////

type psshMock struct {
	logutil.WithLog

	daConfig *config.TDiskAgentConfig
	tmpDir   string
}

func (pssh *psshMock) RunOnHost(
	ctx context.Context,
	cmd []string,
	target string,
) ([]string, error) {
	pssh.LogInfo(ctx, "[PSSH] Run: '%v' on %v", cmd, target)

	if strings.Contains(target, "broken") {
		return nil, errors.New("broken host")
	}

	lines := strings.Split(strings.ReplaceAll(strings.Join(cmd, " && "), " || ", " && "), "&&")

	var response []string

	for _, line := range lines {
		s := strings.TrimSpace(line)

		if strings.HasPrefix(s, "kikimr admin console execute ") {
			if strings.Contains(s, " cms-get-items-") {
				var items []*kikimr.TConfigItem

				code := kikimr.StatusIds_SUCCESS
				kind := uint32(kikimr.TConfigItem_NamedConfigsItem)

				if pssh.daConfig != nil {
					name, _ := MakeConfigItemName(pssh.daConfig)
					data := (&proto.TextMarshaler{Compact: true}).Text(pssh.daConfig)

					items = append(items, &kikimr.TConfigItem{
						Kind: &kind,
						Config: &kikimr.TAppConfig{
							NamedConfigs: []*kikimr.TNamedConfig{
								&kikimr.TNamedConfig{
									Name:   &name,
									Config: []byte(data),
								},
							},
						},
					})
				}

				resp := &kikimr.TConsoleResponse{
					Status: &kikimr.TStatus{
						Code: &code,
					},
					Response: &kikimr.TConsoleResponse_GetConfigItemsResponse{
						GetConfigItemsResponse: &kikimr.TGetConfigItemsResponse{
							Status: &kikimr.TStatus{
								Code: &code,
							},
							ConfigItems: items,
						},
					},
				}

				response = append(response, proto.MarshalTextString(resp))

				continue
			}

			i := strings.Index(s, " cms-update-item-")
			if i != -1 {
				in, err := ioutil.ReadFile(path.Join(pssh.tmpDir, s[i+1:]))
				if err != nil {
					panic(err)
				}
				req := &kikimr.TConsoleRequest{}
				err = proto.UnmarshalText(string(in), req)
				if err != nil {
					panic(err)
				}

				expectedName, _ := MakeConfigItemName(pssh.daConfig)

				r := req.Request.(*kikimr.TConsoleRequest_ConfigureRequest)
				for _, a := range r.ConfigureRequest.Actions {
					add, ok := a.Action.(*kikimr.TConfigureAction_AddConfigItem)
					if !ok {
						continue
					}

					if *add.AddConfigItem.ConfigItem.Kind != uint32(kikimr.TConfigItem_NamedConfigsItem) {
						continue
					}

					for _, item := range add.AddConfigItem.ConfigItem.Config.NamedConfigs {
						if *item.Name != expectedName {
							continue
						}
						pssh.daConfig = &config.TDiskAgentConfig{}

						err := proto.UnmarshalText(string(item.Config), pssh.daConfig)
						if err != nil {
							panic(err)
						}

						response = append(
							response,
							`ConfigureResponse {
								Status { Code: SUCCESS }
								AddedItemIds: 42
							}
							Status {
								Code: SUCCESS
							}`,
						)
					}
				}

				continue
			}

			if strings.Contains(s, " cms-remove-items-") {
				response = append(
					response,
					`ConfigureResponse {
						Status { Code: SUCCESS }
						AddedItemIds: 42
					}
					Status {
						Code: SUCCESS
					}`,
				)

				continue
			}
		}

		response = append(response, "unknown command")
	}

	return response, nil
}

func (pssh *psshMock) Run(
	ctx context.Context,
	cmd []string,
	targets []string,
) map[string]ssh.HostResult {
	result := make(map[string]ssh.HostResult)
	for _, host := range targets {
		output, err := pssh.RunOnHost(ctx, cmd, host)
		result[host] = ssh.HostResult{
			Output: output,
			Err:    err,
		}
	}
	return result
}

func (pssh *psshMock) CopyFile(
	ctx context.Context,
	source string,
	target string,
) error {
	pssh.LogInfo(ctx, "[PSSH] CopyFile: '%v' to %v", source, target)

	if strings.Contains(target, "broken") {
		return errors.New("broken host")
	}

	return nil
}

func (pssh *psshMock) ListHosts(
	ctx context.Context,
	target string,
) ([]string, error) {
	return []string{target}, nil
}

////////////////////////////////////////////////////////////////////////////////

func TestGetConfigs(t *testing.T) {
	assert := assert.New(t)

	tmpDir, err := ioutil.TempDir(os.TempDir(), "nbs_cms_")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := nbs.NewLog(
		log.New(
			os.Stdout,
			"",
			0,
		),
		nbs.LOG_DEBUG,
	)

	agentID := "sas09-ct7-25.cloud.yandex.net"

	var client = NewClient(
		logger,
		"C@cloud_hw-nbs-stable-lab_compute[0]",
		"vasya",
		tmpDir,
		pssh.NewDurable(
			logger,
			&psshMock{
				WithLog: logutil.WithLog{Log: logger},
				daConfig: &config.TDiskAgentConfig{
					AgentId: &agentID,
				},
				tmpDir: tmpDir,
			},
			5,
		),
	)

	items, err := client.GetConfigs(context.TODO(), agentID)
	require.NoError(t, err)

	assert.Equal(1, len(items))
	da := items[0].(*config.TDiskAgentConfig)
	assert.Equal(agentID, *da.AgentId)
}

func TestUpdateConfigs(t *testing.T) {
	assert := assert.New(t)

	tmpDir, err := ioutil.TempDir(os.TempDir(), "nbs_cms_")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(tmpDir) }()

	logger := nbs.NewLog(
		log.New(
			os.Stdout,
			"",
			0,
		),
		nbs.LOG_DEBUG,
	)

	agentID := "sas09-ct7-25.cloud.yandex.net"

	var client = NewClient(
		logger,
		"C@cloud_hw-nbs-stable-lab_compute[0]",
		"vasya",
		tmpDir,
		&psshMock{
			WithLog: logutil.WithLog{Log: logger},
			tmpDir:  tmpDir,
		},
	)

	items, err := client.GetConfigs(context.TODO(), agentID)
	require.NoError(t, err)

	assert.Equal(0, len(items))

	err = client.UpdateConfig(
		context.TODO(),
		agentID,
		func() proto.Message {
			enabled := true
			return &config.TDiskAgentConfig{
				AgentId: &agentID,
				Enabled: &enabled,
			}
		}(),
		"test-cookie",
	)
	require.NoError(t, err)

	items, err = client.GetConfigs(context.TODO(), agentID)
	require.NoError(t, err)

	assert.Equal(1, len(items))
	da := items[0].(*config.TDiskAgentConfig)
	assert.Equal(agentID, *da.AgentId)
	assert.True(*da.Enabled)
}
