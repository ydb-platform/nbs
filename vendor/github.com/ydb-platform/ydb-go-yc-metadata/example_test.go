package yc_test

import (
	"context"

	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
	yc "github.com/ydb-platform/ydb-go-yc-metadata"
)

func ExampleNewInstanceServiceAccount() {
	db, err := ydb.Open(context.TODO(), "grpc://localhost:2136/local",
		ydb.WithCredentials(
			yc.NewInstanceServiceAccount(
				yc.WithURL("http://localhost:6770/meatadata/v1/instance/service-accounts/default/token"),
			),
		),
	)
	if err != nil {
		panic(err)
	}
	_ = db.Close(context.TODO())
}
