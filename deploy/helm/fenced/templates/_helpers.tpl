{{/*
Expand the name of the chart.
*/}}
{{- define "fenced.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "fenced.fullname" -}}
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

{{/* The Control API has no Kubernetes API permissions to grant. */}}
{{- define "fenced.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "fenced.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* Both Deployments and Services must be scoped to this release. */}}
{{- define "fenced.controlSelectorLabels" -}}
app.kubernetes.io/name: fenced-control
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- end -}}

{{/* Empty production credentials must fail before any workload is installed. */}}
{{- define "fenced.validateProduction" -}}
{{- $issuer := required "security.oidc.issuer is required" (.Values.security.oidc.issuer | trim) -}}
{{- if not (hasPrefix "https://" $issuer) -}}
{{- fail "security.oidc.issuer must use HTTPS" -}}
{{- end -}}
{{- $clientID := required "security.oidc.clientID is required" (.Values.security.oidc.clientID | trim) -}}
{{- $tenantClaim := required "security.oidc.tenantClaim is required" (.Values.security.oidc.tenantClaim | trim) -}}
{{- $databaseSecret := required "database.existingSecret is required" (.Values.database.existingSecret | trim) -}}
{{- $databaseKey := required "database.secretKey is required" (.Values.database.secretKey | trim) -}}
{{- $tlsSecret := required "security.tls.existingSecret is required" (.Values.security.tls.existingSecret | trim) -}}
{{- $tlsCertKey := required "security.tls.certificateKey is required" (.Values.security.tls.certificateKey | trim) -}}
{{- $tlsPrivateKey := required "security.tls.privateKeyKey is required" (.Values.security.tls.privateKeyKey | trim) -}}
{{- if eq $tlsCertKey $tlsPrivateKey -}}
{{- fail "security.tls.certificateKey and privateKeyKey must be distinct" -}}
{{- end -}}
{{- $auditSecret := required "security.audit.existingSecret is required" (.Values.security.audit.existingSecret | trim) -}}
{{- $auditKey := required "security.audit.privateKeyKey is required" (.Values.security.audit.privateKeyKey | trim) -}}
{{- $auditKeyID := required "security.audit.keyID is required" (.Values.security.audit.keyID | trim) -}}
{{- $trustSecret := required "security.packageTrust.existingSecret is required" (.Values.security.packageTrust.existingSecret | trim) -}}
{{- $trustKey := required "security.packageTrust.keysKey is required" (.Values.security.packageTrust.keysKey | trim) -}}
{{- $embeddingEndpoint := required "embedding.endpoint is required" (.Values.embedding.endpoint | trim) -}}
{{- if not (hasPrefix "https://" $embeddingEndpoint) -}}
{{- fail "embedding.endpoint must use HTTPS" -}}
{{- end -}}
{{- if .Values.embedding.tokenSecret -}}
{{- $embeddingTokenKey := required "embedding.tokenSecretKey is required when tokenSecret is set" (.Values.embedding.tokenSecretKey | trim) -}}
{{- end -}}
{{- end -}}
