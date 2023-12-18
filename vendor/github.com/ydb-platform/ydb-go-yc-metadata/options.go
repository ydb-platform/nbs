package yc

import (
	ydb "github.com/ydb-platform/ydb-go-sdk/v3"
)

func WithURL(url string) InstanceServiceAccountCredentialsOption {
	return WithInstanceServiceAccountURL(url)
}

func WithCredentials(opts ...InstanceServiceAccountCredentialsOption) ydb.Option {
	return ydb.WithCredentials(
		NewInstanceServiceAccount(opts...),
	)
}

func WithInternalCA() ydb.Option {
	return ydb.WithCertificatesFromPem(ycPEM)
}
