{{- define "redis-cluster-kubernetes-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "redis-cluster-kubernetes-operator.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "redis-cluster-kubernetes-operator.name" . -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "redis-cluster-kubernetes-operator.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | quote }}
app.kubernetes.io/name: {{ include "redis-cluster-kubernetes-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "redis-cluster-kubernetes-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "redis-cluster-kubernetes-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: operator
{{- end -}}

{{- define "redis-cluster-kubernetes-operator.toolboxSelectorLabels" -}}
app.kubernetes.io/name: {{ include "redis-cluster-kubernetes-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: toolbox
{{- end -}}

{{- define "redis-cluster-kubernetes-operator.operatorServiceAccountName" -}}
{{- default (include "redis-cluster-kubernetes-operator.fullname" .) .Values.operator.serviceAccount.name -}}
{{- end -}}

{{- define "redis-cluster-kubernetes-operator.toolboxServiceAccountName" -}}
{{- default (include "redis-cluster-kubernetes-operator.toolboxFullname" .) .Values.toolbox.serviceAccount.name -}}
{{- end -}}

{{- define "redis-cluster-kubernetes-operator.toolboxFullname" -}}
{{- printf "%s-toolbox" (include "redis-cluster-kubernetes-operator.fullname" . | trunc 55 | trimSuffix "-") -}}
{{- end -}}

{{- define "redis-cluster-kubernetes-operator.metricsFullname" -}}
{{- printf "%s-metrics" (include "redis-cluster-kubernetes-operator.fullname" . | trunc 55 | trimSuffix "-") -}}
{{- end -}}

{{- define "redis-cluster-kubernetes-operator.clusterPort" -}}
{{- add 10000 (int64 .Values.port) -}}
{{- end -}}

{{- define "redis-cluster-kubernetes-operator.redisAuthHelperName" -}}
{{- $source := .Files.Get "files/redis-operator-redis-cli" -}}
{{- printf "%s-auth-%s" (include "redis-cluster-kubernetes-operator.fullname" . | trunc 45 | trimSuffix "-") (sha256sum $source | trunc 12) -}}
{{- end -}}
