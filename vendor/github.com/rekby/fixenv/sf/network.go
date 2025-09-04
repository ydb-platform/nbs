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
	f := func() (*fixenv.Result, error) {
		listener, err := net.Listen("tcp", "localhost:0")
		clean := func() {
			if listener != nil {
				_ = listener.Close()
			}
		}
		return fixenv.NewResultWithCleanup(listener, clean), err
	}
	return e.CacheResult(f, fixenv.CacheOptions{CacheKey: name}).(*net.TCPListener)
}
