{{- define "common.names.fullname" -}}
    {{- printf "%s-%s" "disk-manager" .Values.serviceKind -}}
{{- end -}}