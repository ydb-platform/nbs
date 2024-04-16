{{- define "common.names.fullname" -}}
    {{- printf "%s-%s" "disk-manager" .Values.serviceKind -}}
{{- end -}}
{{- define "common.names.configname" -}}
    {{ $name := include "common.names.fullname" . }}
    {{- printf "%s-%s" $name "config" -}}
{{- end -}}

{{- define "disk-manager.serverConfigName" -}}
server-config.txt
{{- end -}}
{{- define "disk-manager.clientConfigName" -}}
client-config.txt
{{- end -}}

{{- define "disk-manager.serverConfigPath" -}}
/etc/yc/disk-manager/{{ include "disk-manager.serverConfigName" . }}
{{- end -}}
{{- define "disk-manager.clientConfigPath" -}}
/etc/yc/disk-manager/{{ include "disk-manager.clientConfigName" . }}
{{- end -}}

{{- define "disk-manager.serverVolumeName" -}}
disk-manager-server-config
{{- end -}}
{{- define "disk-manager.clientVolumeName" -}}
disk-manager-client-config
{{- end -}}

{{- define  "disk-manager.clientConfigFile" -}}
    {{ tpl (.Files.Get "files/configs/client.txt") . }}
{{- end -}}
{{- define  "disk-manager.serverConfigFile" -}}
    {{ tpl (.Files.Get "files/configs/server.txt") . }}
{{- end -}}