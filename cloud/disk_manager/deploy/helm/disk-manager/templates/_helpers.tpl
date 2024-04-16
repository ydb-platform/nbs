{{- define "common.names.fullname" -}}
    {{- printf "%s-%s" "disk-manager" .Values.serviceKind -}}
{{- end -}}
{{- define "common.names.configname" -}}
    {{ $name := include "common.names.fullname" . }}
    {{- printf "%s-%s" $name "config" -}}
{{- end -}}