package metrics

import (
	"fmt"
	"net/http"

	"github.com/ydb-platform/nbs/library/go/core/metrics/solomon"
	"github.com/ydb-platform/nbs/library/go/yandex/solomon/reporters/puller/httppuller"
)

////////////////////////////////////////////////////////////////////////////////

func NewSolomonRegistry(
	mux *http.ServeMux,
	path string,
) Registry {

	registry := solomon.NewRegistry(solomon.NewRegistryOpts().SetRated(true))
	mux.Handle(fmt.Sprintf("/solomon/%v", path), httppuller.NewHandler(registry))

	return registry
}
