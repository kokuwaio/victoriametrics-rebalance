{{/*
Expand the name of the chart.
*/}}
{{- define "vmrebalance.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "vmrebalance.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "vmrebalance.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "vmrebalance.labels" -}}
helm.sh/chart: {{ include "vmrebalance.chart" . }}
{{ include "vmrebalance.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "vmrebalance.selectorLabels" -}}
app.kubernetes.io/name: {{ include "vmrebalance.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "vmrebalance.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "vmrebalance.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
The image, with the tag defaulting to v<appVersion>.
*/}}
{{- define "vmrebalance.image" -}}
{{- printf "%s:%s" .Values.image.repository (.Values.image.tag | default (printf "v%s" .Chart.AppVersion)) }}
{{- end }}

{{/*
The Job's name: one Job per release revision, so every upgrade starts a new run.
*/}}
{{- define "vmrebalance.jobName" -}}
{{- printf "%s-%d" (include "vmrebalance.fullname" . | trunc 52 | trimSuffix "-") .Release.Revision }}
{{- end }}

{{/*
The vmrebalance arguments, as a YAML list. toYaml quotes every value that needs it.
*/}}
{{- define "vmrebalance.args" -}}
{{- $v := .Values -}}
{{- $args := list "migrate"
  (printf "--namespace=%s" .Release.Namespace)
  (printf "--from=%s" (required "from (the old VMCluster) is required" $v.from))
  (printf "--to=%s" (required "to (the new VMCluster) is required" $v.to))
  (printf "--tenant=%v" $v.tenant)
  (printf "--concurrency=%d" (int $v.copy.concurrency))
  (printf "--step=%s" $v.copy.step)
  (printf "--retries=%d" (int $v.copy.retries))
  (printf "--backoff=%s" $v.copy.backoff)
  (printf "--final-overlap=%s" $v.final.overlap)
  (printf "--final-tail=%s" $v.final.tail)
  (printf "--drain-interval=%s" $v.final.drainInterval)
  (printf "--drain-timeout=%s" $v.final.drainTimeout)
  (printf "--top-n=%d" (int $v.verify.topN))
  (printf "--random-n=%d" (int $v.verify.randomN))
  (printf "--pool=%d" (int $v.verify.pool))
  (printf "--tolerance=%v" $v.verify.tolerance)
  (printf "--headroom=%v" $v.migrate.headroom)
  (printf "--poll-interval=%s" $v.migrate.pollInterval)
  (printf "--ready-after=%s" $v.migrate.readyAfter) }}
{{- with $v.copy.rateLimit }}
{{- $args = append $args (printf "--rate-limit=%s" .) }}
{{- end }}
{{- if $v.copy.adaptive }}
{{- if not $v.copy.rateLimit }}
{{- fail "copy.adaptive needs copy.rateLimit as the highest rate" }}
{{- end }}
{{- $args = append $args "--adaptive" }}
{{- $args = append $args (printf "--max-select-utilization=%v" $v.copy.maxSelectUtilization) }}
{{- with $v.copy.minRateLimit }}
{{- $args = append $args (printf "--min-rate-limit=%s" .) }}
{{- end }}
{{- with $v.copy.maxQueryLatency }}
{{- $args = append $args (printf "--max-query-latency=%s" .) }}
{{- end }}
{{- end }}
{{- with $v.migrate.since }}
{{- $args = append $args (printf "--since=%s" .) }}
{{- end }}
{{- with $v.aliases.select }}
{{- $args = append $args (printf "--select-alias=%s" .) }}
{{- end }}
{{- with $v.aliases.insert }}
{{- $args = append $args (printf "--insert-alias=%s" .) }}
{{- end }}
{{- if $v.metrics.enabled }}
{{- $args = append $args (printf "--metrics-addr=:%d" (int $v.metrics.port)) }}
{{- end }}
{{- range $v.extraArgs }}
{{- $args = append $args (toString .) }}
{{- end }}
{{- toYaml $args }}
{{- end }}
