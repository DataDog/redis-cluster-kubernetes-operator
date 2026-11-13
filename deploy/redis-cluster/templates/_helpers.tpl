{{- define "redis-cluster.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- default .Release.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/* Selector labels required by the workload contract. */}}
{{- define "redis-cluster.selectorLabels" -}}
app: redis-cluster
chart_name: {{ include "redis-cluster.fullname" . }}
app.kubernetes.io/name: redis-cluster
redis_cluster_name: {{ include "redis-cluster.fullname" . }}
{{- end -}}
