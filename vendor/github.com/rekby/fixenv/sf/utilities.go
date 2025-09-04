package sf

import "github.com/rekby/fixenv"

func mustNoErr(e fixenv.Env, err error, format string, args ...interface{}) {
	if err == nil {
		return
	}
	e.T().Fatalf(format, args...)
}
