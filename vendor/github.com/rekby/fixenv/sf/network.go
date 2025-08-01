package sf

import (
	"github.com/rekby/fixenv"
	"net"
)

func FreeLocalTCPAddress(e fixenv.Env) string {
	return FreeLocalTCPAddressNamed(e, "")
}

func FreeLocalTCPAddressNamed(e fixenv.Env, name string) string {
	f := func() (*fixenv.Result, error) {
		listener := LocalTCPListenerNamed(e, "FreeLocalTCPAddressNamed-"+name)
		addr := listener.Addr().String()
		err := listener.Close()
		mustNoErr(e, err, "failed to close temp listener: %v", err)
		return fixenv.NewResult(addr), nil
	}
	return e.CacheResult(f, fixenv.CacheOptions{CacheKey: name}).(string)
}

func LocalTCPListener(e fixenv.Env) *net.TCPListener {
	return LocalTCPListenerNamed(e, "")
}

func LocalTCPListenerNamed(e fixenv.Env, name string) *net.TCPListener {
	return e.CacheWithCleanup(name, nil, func() (res interface{}, cleanup fixenv.FixtureCleanupFunc, err error) {
		listener, err := net.Listen("tcp", "localhost:0")
		clean := func() {
			_ = listener.Close()
		}
		return listener, clean, err
	}).(*net.TCPListener)
}
