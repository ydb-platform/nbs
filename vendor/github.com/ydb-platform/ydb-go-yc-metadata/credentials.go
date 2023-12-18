package yc

// NewInstanceServiceAccount makes credentials provider that uses instance metadata url to obtain
// token for service account attached to instance. Cancelling context will lead to credentials
// refresh halt. It should be used during application stop or credentials recreation.
func NewInstanceServiceAccount(opts ...InstanceServiceAccountCredentialsOption) *InstanceServiceAccountCredentials {
	return instanceServiceAccount(
		append(
			[]InstanceServiceAccountCredentialsOption{
				WithInstanceServiceAccountCredentialsSourceInfo("yc.NewInstanceServiceAccount(ctx)"),
			},
			opts...,
		)...,
	)
}
