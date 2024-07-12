GO_LIBRARY()

SUBSCRIBER(g:go-contrib)

LICENSE(MPL-2.0)

SRCS(
    datasource.go
    datasourceconfigvalidator.go
    datasourcewithconfigure.go
    datasourcewithconfigvalidators.go
    datasourcewithvalidateconfig.go
    doc.go
    provider.go
    providerconfigvalidator.go
    providerwithconfigvalidators.go
    providerwithmetaschema.go
    providerwithvalidateconfig.go
    resource.go
    resourceconfigvalidator.go
    resourcewithconfigure.go
    resourcewithconfigureandimportstate.go
    resourcewithconfigureandmodifyplan.go
    resourcewithconfigureandupgradestate.go
    resourcewithconfigvalidators.go
    resourcewithimportstate.go
    resourcewithmodifyplan.go
    resourcewithupgradestate.go
    resourcewithvalidateconfig.go
)

END()
