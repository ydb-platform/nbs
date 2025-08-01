package sf

import (
	"github.com/rekby/fixenv"
	"net/http/httptest"
)

func HTTPServer(e fixenv.Env) *httptest.Server {
	f := func() (*fixenv.Result, error) {
		server := httptest.NewServer(nil)
		return fixenv.NewResultWithCleanup(server, server.Close), nil
	}
	return e.CacheResult(f).(*httptest.Server)
}
