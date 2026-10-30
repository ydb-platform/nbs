package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	api "github.com/ydb-platform/nbs/contrib/ydb/public/api/client/yc_private/compute"
	compute "github.com/ydb-platform/nbs/contrib/ydb/public/api/client/yc_private/compute/inner"
)

func TestExportCredentialsSelectResourceIndependentlyOfTask(t *testing.T) {
	service := &computeService{
		s3Credentials: map[string]*api.ExportS3Credentials{
			"snapshot/same-id": {AccessKeyId: "snapshot-key", SecretAccessKey: "s"},
			"image/same-id":    {AccessKeyId: "image-key", SecretAccessKey: "i"},
		},
	}
	for _, taskID := range []string{"task-1", "task-2"} {
		for _, image := range []bool{false, true} {
			req := &compute.GetExportCredentialsRequest{TaskId: taskID}
			expected := "snapshot-key"
			if image {
				req.ImageId = "same-id"
				expected = "image-key"
			} else {
				req.SnapshotId = "same-id"
			}

			response, err := service.GetExportCredentials(context.Background(), req)
			if err != nil {
				t.Fatal(err)
			}

			if response.GetCredentials().GetAccessKeyId() != expected {
				t.Fatal("credentials must be selected by resource, not task")
			}
			response.Credentials.AccessKeyId = "changed-response"
		}
	}
}

func TestExportCredentialsRejectUnknownOrAmbiguousResource(t *testing.T) {
	service := &computeService{
		s3Credentials: map[string]*api.ExportS3Credentials{},
	}
	for _, test := range []struct {
		request *compute.GetExportCredentialsRequest
		code    codes.Code
	}{
		{&compute.GetExportCredentialsRequest{}, codes.InvalidArgument},
		{&compute.GetExportCredentialsRequest{
			SnapshotId: "snapshot", ImageId: "image",
		}, codes.InvalidArgument},
		{&compute.GetExportCredentialsRequest{SnapshotId: "missing"}, codes.NotFound},
	} {
		_, err := service.GetExportCredentials(context.Background(), test.request)
		if status.Code(err) != test.code {
			t.Fatalf("expected %v, got %v", test.code, err)
		}
	}
}

func TestExportCredentialsConfigValidatesMultipleEntries(t *testing.T) {
	entry := `ExportCredentials {
    SnapshotId: "same-id"
    Credentials {
        access_key_id: "key"
        secret_access_key: "secret-not-for-logs"
    }
}`
	for _, test := range []struct {
		config string
		valid  bool
	}{
		{entry, true},
		{entry + "\n" + strings.Replace(entry, "SnapshotId", "ImageId", 1), true},
		{entry + "\n" + entry, false},
	} {
		path := filepath.Join(t.TempDir(), "config.txt")
		if err := os.WriteFile(path, []byte(test.config), 0600); err != nil {
			t.Fatal(err)
		}

		_, err := parseConfig(path)
		if (err == nil) != test.valid {
			t.Fatalf("unexpected validation result: %v", err)
		}
		if err != nil && strings.Contains(err.Error(), "secret-not-for-logs") {
			t.Fatal("configuration errors must not contain credentials")
		}
	}
}
