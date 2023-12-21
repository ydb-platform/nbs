package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	nbs "a.yandex-team.ru/cloud/blockstore/public/sdk/go/client"
)

////////////////////////////////////////////////////////////////////////////////

func makeClient(
	host string,
	port int,
	token string,
) (*nbs.Client, error) {
	var grpcOpts nbs.GrpcClientOpts
	grpcOpts.Endpoint = fmt.Sprintf("%v:%v", host, port)
	if len(token) > 0 {
		grpcOpts.Credentials = &nbs.ClientCredentials{
			AuthToken: token,
		}
	}

	var durableOpts nbs.DurableClientOpts

	return nbs.NewClient(
		&grpcOpts,
		&durableOpts,
		nbs.NewStderrLog(nbs.LOG_INFO),
	)
}

////////////////////////////////////////////////////////////////////////////////

func backupDiskRegistryState(
	ctx context.Context,
	client *nbs.Client,
) ([]byte, error) {
	return client.ExecuteAction(ctx, "backupdiskregistrystate", []byte("{}"))
}

////////////////////////////////////////////////////////////////////////////////

type DisableAgentRequest struct {
	AgentID     string   `json:"AgentId"`
	DeviceUUIDs []string `json:"DeviceUUIDs"`
}

type ChangeDeviceStateRequest struct {
	DeviceUUID string `json:"DeviceUUID"`
	State      uint32 `json:"State"`
}

type ChangeStateRequest struct {
	DisableAgent      *DisableAgentRequest      `json:"DisableAgent"`
	ChangeDeviceState *ChangeDeviceStateRequest `json:"ChangeDeviceState"`
	Message           string                    `json:"Message"`
}

////////////////////////////////////////////////////////////////////////////////

func target2String(target *Target) string {
	var sb strings.Builder

	sb.WriteString("DiskId=")
	sb.WriteString(target.DiskID)
	for _, deviceUUID := range target.DeviceUUIDs {
		sb.WriteString(" DeviceUUID=")
		sb.WriteString(deviceUUID)
	}
	sb.WriteString(" AgentID=")
	sb.WriteString(target.AgentID)

	return sb.String()
}

////////////////////////////////////////////////////////////////////////////////

func applyTargets(
	ctx context.Context,
	targets []Target,
	client *nbs.Client,
) ([]string, error) {
	var responses []string

	for _, target := range targets {
		bytes, err := json.Marshal(ChangeStateRequest{
			DisableAgent: &DisableAgentRequest{
				AgentID:     target.AgentID,
				DeviceUUIDs: target.DeviceUUIDs,
			},
			Message: fmt.Sprintf(
				"chaos-monkey: disabling target %v",
				target2String(&target),
			),
		})
		if err != nil {
			return nil, err
		}

		response, err := client.ExecuteAction(
			ctx,
			"diskregistrychangestate",
			bytes,
		)
		if err != nil {
			return nil, err
		}

		responses = append(responses, string(response))
	}

	return responses, nil
}

////////////////////////////////////////////////////////////////////////////////

func waitForReplicationStart(
	ctx context.Context,
	targets []Target,
	client *nbs.Client,
) ([]string, error) {
	var responses []string

	for _, target := range targets {
		for {
			volume, _, err := client.StatVolume(ctx, target.DiskID, 0)

			if err != nil {
				return nil, err
			}

			if len(volume.FreshDeviceIds) != 0 {
				break
			}

			time.Sleep(time.Second)

			// TODO: limit maximum waiting time
		}

		responses = append(responses, fmt.Sprintf("Disk %v done", target.DiskID))
	}

	return responses, nil
}

////////////////////////////////////////////////////////////////////////////////

func waitForReplicationFinish(
	ctx context.Context,
	targets []Target,
	client *nbs.Client,
) ([]string, error) {
	logger := log.New(os.Stderr, "REPLICATION_FINISH ", 0)

	var responses []string

	for _, target := range targets {
		lastFreshDeviceCount := 0

		for {
			volume, _, err := client.StatVolume(ctx, target.DiskID, 0)

			if err != nil {
				return nil, err
			}

			if len(volume.FreshDeviceIds) == 0 {
				break
			}

			if lastFreshDeviceCount != len(volume.FreshDeviceIds) {
				lastFreshDeviceCount = len(volume.FreshDeviceIds)
				logger.Printf(
					"Disk %v, remaining fresh devices: %v / %v",
					target.DiskID,
					len(volume.FreshDeviceIds),
					len(volume.Devices),
				)
			}

			time.Sleep(time.Second)

			// TODO: limit maximum waiting time
		}

		responses = append(
			responses,
			fmt.Sprintf("Disk %v done", target.DiskID),
		)
	}

	return responses, nil
}

////////////////////////////////////////////////////////////////////////////////

func cleanup(
	ctx context.Context,
	state *DiskRegistryStateBackup,
	client *nbs.Client,
) ([]string, error) {
	var outputs []string

	for _, agent := range state.Backup.Agents {
		if agent.State == "AGENT_STATE_UNAVAILABLE" {
			outputs = append(
				outputs,
				fmt.Sprintf("Unavailable agent %v", agent.AgentID),
			)
		}

		for _, device := range agent.Devices {
			if device.State == "DEVICE_STATE_ERROR" &&
				// XXX relying on message prefix
				strings.HasPrefix(device.StateMessage, "MirroredDisk") {

				bytes, err := json.Marshal(ChangeStateRequest{
					ChangeDeviceState: &ChangeDeviceStateRequest{
						DeviceUUID: device.DeviceUUID,
						State:      0, // DEVICE_STATE_ONLINE
					},
					Message: fmt.Sprintf(
						"chaos-monkey: restoring device state"+
							", prev state message: %v",
						device.StateMessage,
					),
				})
				if err != nil {
					return nil, err
				}

				response, err := client.ExecuteAction(
					ctx,
					"diskregistrychangestate",
					bytes,
				)
				if err != nil {
					return nil, err
				}

				outputs = append(
					outputs,
					fmt.Sprintf(
						"restored state for device %v: %v",
						device.DeviceUUID,
						string(response),
					),
				)
			}
		}
	}

	return outputs, nil
}

func heal(
	ctx context.Context,
	brokenDevices []BriefDeviceInfo,
	client *nbs.Client,
) ([]string, error) {
	var outputs []string

	for _, device := range brokenDevices {
		bytes, err := json.Marshal(ChangeStateRequest{
			ChangeDeviceState: &ChangeDeviceStateRequest{
				DeviceUUID: device.DeviceUUID,
				State:      0, // DEVICE_STATE_ONLINE
			},
			Message: fmt.Sprintf(
				"chaos-monkey: restoring device state"+
					", prev state message: %v",
				device.StateMessage,
			),
		})
		if err != nil {
			return nil, err
		}

		response, err := client.ExecuteAction(
			ctx,
			"diskregistrychangestate",
			bytes,
		)
		if err != nil {
			return nil, err
		}

		outputs = append(
			outputs,
			fmt.Sprintf(
				"restored state for device %v: %v",
				device.DeviceUUID,
				string(response),
			),
		)

	}

	return outputs, nil
}
