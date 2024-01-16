package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type ycpWrapper struct {
	profile    string
	folderID   string
	configPath string
}

////////////////////////////////////////////////////////////////////////////////

func (ycp *ycpWrapper) execImpl(
	ctx context.Context,
	command string,
	params map[string]interface{},
	async bool,
) (map[string]interface{}, error) {

	logging.Debug(ctx, "ycp exec command: %v, params: %v", command, params)

	f, err := os.CreateTemp("", "request")
	if err != nil {
		return nil, err
	}
	defer os.Remove(f.Name())

	for k, v := range params {
		_, err := fmt.Fprintf(f, "%v: %v\n", k, v)
		if err != nil {
			return nil, err
		}
	}

	if strings.Contains(command, "create") {
		_, err := fmt.Fprintf(f, "folder_id: %v\n", ycp.folderID)
		if err != nil {
			return nil, err
		}
	}

	args := strings.Split(command, " ")
	args = append(args, []string{
		"--profile",
		ycp.profile,
		"-r",
		f.Name(),
		"--format",
		"json",
	}...)
	if async {
		args = append(args, "--async")
	}
	if len(ycp.configPath) > 0 {
		args = append(args, "--config-path", ycp.configPath)
	}

	cmd := exec.CommandContext(ctx, "ycp", args...)

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}

	err = cmd.Start()
	if err != nil {
		return nil, err
	}

	stderrBytes, _ := io.ReadAll(stderr)
	stdoutBytes, _ := io.ReadAll(stdout)

	err = cmd.Wait()
	if err != nil {
		return nil, fmt.Errorf(
			"ycp exec failed: %v: %v",
			err,
			string(stderrBytes),
		)
	}

	var result map[string]interface{}
	err = json.Unmarshal(stdoutBytes, &result)
	if err != nil {
		return nil, err
	}

	logging.Debug(ctx, "ycp exec result: %v", result)

	return result, nil
}

////////////////////////////////////////////////////////////////////////////////

func (ycp *ycpWrapper) exec(
	ctx context.Context,
	command string,
	params map[string]interface{},
) (map[string]interface{}, error) {

	return ycp.execImpl(ctx, command, params, false)
}

func (ycp *ycpWrapper) execAsync(
	ctx context.Context,
	command string,
	params map[string]interface{},
) (map[string]interface{}, error) {

	return ycp.execImpl(ctx, command, params, true)
}

////////////////////////////////////////////////////////////////////////////////

func newYcpWrapper(
	profile string,
	folderID string,
	ycpConfigPath string,
) *ycpWrapper {

	return &ycpWrapper{
		profile:    profile,
		folderID:   folderID,
		configPath: ycpConfigPath,
	}
}
