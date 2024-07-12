// Copyright 2021 The go-github AUTHORS. All rights reserved.
//
// `Use` of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package github

import (
    "context"
    "fmt"
    "net/http"
    "testing"
    "time"

    "github.com/google/go-cmp/cmp"
)

func TestOrganizationService_GetAuditLog(t *testing.T) {
    client, mux, _, teardown := setup()
    defer teardown()

    mux.HandleFunc("/orgs/o/audit-log", func(w http.ResponseWriter, r *http.Request) {
        testMethod(t, r, "GET")

        fmt.Fprint(w, `[
        {
        "active": true,
        "workflow_id": 123456,
        "head_branch": "master",
        "org": "o",
        "trigger_id": null,
        "repo": "o/blue-crayon-1",
        "created_at": 1615077308538,
        "head_sha": "5acdeadbeef64d1a62388e901e5cdc9358644b37",
        "conclusion": "success",
        "actor": "testactor",
        "completed_at": "2021-03-07T00:35:08.000Z",
        "@timestamp": 1615077308538,
        "name": "Code scanning - action",
        "action": "workflows.completed_workflow_run",
        "started_at": "2021-03-07T00:33:04.000Z",
        "event": "schedule",
        "workflow_run_id": 628312345,
        "_document_id": "beeZYapIUe-wKg5-beadb33",
        "config": {
            "content_type": "json",
            "insecure_ssl": "0",
            "url": "https://example.com/deadbeef-new-hook"
         },
        "events": ["code_scanning_alert"]
        }]`)
    })
    ctx := context.Background()
    getOpts := GetAuditLogOptions{
        Include: String("all"),
        Phrase:  String("action:workflows"),
        Order:   String("asc"),
    }

    auditEntries, _, err := client.Organizations.GetAuditLog(ctx, "o", &getOpts)
    if err != nil {
        t.Errorf("Organizations.GetAuditLog returned error: %v", err)
    }
    startedAt, _ := time.Parse(time.RFC3339, "2021-03-07T00:33:04.000Z")
    completedAt, _ := time.Parse(time.RFC3339, "2021-03-07T00:35:08.000Z")
    timestamp := time.Unix(0, 1615077308538*1e6)

    want := []*AuditEntry{
        {
            Timestamp:     &Timestamp{timestamp},
            DocumentID:    String("beeZYapIUe-wKg5-beadb33"),
            Action:        String("workflows.completed_workflow_run"),
            Actor:         String("testactor"),
            Active:        Bool(true),
            CompletedAt:   &Timestamp{completedAt},
            Conclusion:    String("success"),
            CreatedAt:     &Timestamp{timestamp},
            Event:         String("schedule"),
            HeadBranch:    String("master"),
            HeadSHA:       String("5acdeadbeef64d1a62388e901e5cdc9358644b37"),
            Name:          String("Code scanning - action"),
            Org:           String("o"),
            Repo:          String("o/blue-crayon-1"),
            StartedAt:     &Timestamp{startedAt},
            WorkflowID:    Int64(123456),
            WorkflowRunID: Int64(628312345),
            Events:        []string{"code_scanning_alert"},
            Config: &HookConfig{
                ContentType: String("json"),
                InsecureSSL: String("0"),
                URL:         String("https://example.com/deadbeef-new-hook"),
            },
        },
    }

    if !cmp.Equal(auditEntries, want) {
        t.Errorf("Organizations.GetAuditLog return \ngot: %+v,\nwant:%+v", auditEntries, want)
    }

    const methodName = "GetAuditLog"
    testBadOptions(t, methodName, func() (err error) {
        _, _, err = client.Organizations.GetAuditLog(ctx, "\n", &getOpts)
        return err
    })

    testNewRequestAndDoFailure(t, methodName, client, func() (*Response, error) {
        got, resp, err := client.Organizations.GetAuditLog(ctx, "o", &GetAuditLogOptions{})
        if got != nil {
            t.Errorf("testNewRequestAndDoFailure %v = %#v, want nil", methodName, got)
        }
        return resp, err
    })

}
