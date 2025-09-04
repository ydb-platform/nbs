package sf

import (
	"github.com/rekby/fixenv"
	"os"
)

// TempDir return path for existet temporary folder
// the folder will remove after test finish with all contents
func TempDir(e fixenv.Env) string {
	f := func() (*fixenv.Result, error) {
		dir, err := os.MkdirTemp("", "fixenv-auto-")
		mustNoErr(e, err, "failed to create temp dir: %v", err)
		e.T().Logf("Temp dir created: %v", dir)
		clean := func() {
			_ = os.RemoveAll(dir)
			e.T().Logf("Temp dir removed: %v", dir)
		}
		return fixenv.NewResultWithCleanup(dir, clean), nil
	}
	return e.CacheResult(f).(string)
}

// TempFile return path to empty existed file in TempDir
func TempFile(e fixenv.Env) string {
	return TempFileNamed(e, "fixenv-auto-")
}

// TempFileNamed return path to empty file in TempDir
// pattern is pattern for os.CreateTemp
func TempFileNamed(e fixenv.Env, pattern string) string {
	f := func() (*fixenv.Result, error) {
		dir := TempDir(e)
		f, err := os.CreateTemp(dir, pattern)
		mustNoErr(e, err, "failed to create temp file: %w", err)
		fName := f.Name()
		err = f.Close()
		mustNoErr(e, err, "failed to close temp file during initialize: %w", err)
		return fixenv.NewResult(fName), nil
	}

	return e.CacheResult(f).(string)
}
