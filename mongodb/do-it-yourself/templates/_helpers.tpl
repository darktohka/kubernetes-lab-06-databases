{{/*
The release name is the resource name. Helm releases are named "mongodb" in
this lab; keeping the name independent of the chart name makes the DNS names
stable and lets the demo chart work with either variant.
*/}}
{{- define "mongodb-manual.fullname" -}}
{{- default .Release.Name .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "mongodb-manual.serviceName" -}}
{{- printf "%s-svc" (include "mongodb-manual.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "mongodb-manual.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: mongodb
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: lab05
app.kubernetes.io/managed-by: {{ .Release.Service }}
app: mongodb
{{- end -}}

{{/*
Selectors are immutable and must contain only these two labels. The "app"
label is a convenience for humans, never a selector.
*/}}
{{- define "mongodb-manual.selectorLabels" -}}
app.kubernetes.io/name: mongodb
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
