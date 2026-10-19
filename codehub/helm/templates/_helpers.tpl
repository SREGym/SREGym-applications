{{- define "codehub.labels" -}}
app.kubernetes.io/name: codehub
app.kubernetes.io/instance: {{ .Release.Name }}
topology.kubernetes.io/region: {{ .Values.region }}
{{- if .Values.deploymentOwner }}
codehub.local/deployment: {{ .Values.deploymentOwner | quote }}
{{- end }}
{{- end -}}
{{- define "codehub.placement" -}}
nodeSelector:
  topology.kubernetes.io/region: {{ .root.Values.region }}
topologySpreadConstraints:
  - maxSkew: 1
    topologyKey: kubernetes.io/hostname
    whenUnsatisfiable: DoNotSchedule
    labelSelector:
      matchLabels:
        app.kubernetes.io/name: codehub
        app.kubernetes.io/instance: {{ .root.Release.Name }}
        app.kubernetes.io/component: {{ .component }}
        {{- if .group }}
        database-group: {{ .group }}
        {{- end }}
{{- end -}}
{{- define "codehub.security" -}}
runAsNonRoot: true
runAsUser: 10001
runAsGroup: 10001
fsGroup: 10001
seccompProfile: {type: RuntimeDefault}
{{- end -}}
{{- define "codehub.env" -}}
- {name: CODEHUB_REGION, value: {{ .Values.region | quote }}}
- {name: CODEHUB_DATA_DIR, value: /var/lib/codehub}
- {name: CODEHUB_MYSQL_HOST, value: mysql-g0-writer-route}
- {name: CODEHUB_MYSQL_PORT, value: "3306"}
- {name: CODEHUB_MYSQL_READ_HOST, value: {{ .Values.mysql.readHost | quote }}}
- {name: CODEHUB_MYSQL_DATABASE, value: {{ .Values.mysql.database | quote }}}
- {name: CODEHUB_MYSQL_USER, value: {{ .Values.mysql.user | quote }}}
- {name: CODEHUB_MYSQL_PASSWORD_FILE, value: /run/credentials/mysql-password}
- {name: CODEHUB_DATABASE_ROUTES_FILE, value: /etc/codehub/database-routes.json}
- {name: CODEHUB_SERVICE_TOKEN_FILE, value: /run/credentials/service-token}
- {name: CODEHUB_BOOTSTRAP_TOKEN_FILE, value: /run/credentials/bootstrap-token}
- {name: CODEHUB_BROKER_URL_FILE, value: /run/credentials/broker-url}
- {name: CODEHUB_REPOSITORY_URL, value: "http://repository:8080"}
- name: CODEHUB_REPOSITORY_PEERS
  {{- $peers := list }}
  {{- range .Values.regions }}
  {{- if ne . $.Values.region }}
  {{- $peers = append $peers (printf "http://repository.codehub-%s.svc.cluster.local:8080" .) }}
  {{- end }}
  {{- end }}
  value: {{ join "," $peers | quote }}
- {name: CODEHUB_SEARCH_URL, value: "http://search:8080"}
- name: CODEHUB_SEARCH_URLS
  {{- $searchURLs := list }}
  {{- $orderedRegions := list .Values.region }}
  {{- range .Values.regions }}
  {{- if ne . $.Values.region }}
  {{- $orderedRegions = append $orderedRegions . }}
  {{- end }}
  {{- end }}
  {{- range $region := $orderedRegions }}
  {{- range $index := until (int $.Values.replicas.search) }}
  {{- $searchURLs = append $searchURLs (printf "http://search-%d.search-headless.codehub-%s.svc.cluster.local:8080" $index $region) }}
  {{- end }}
  {{- end }}
  value: {{ join "," $searchURLs | quote }}
- {name: CODEHUB_DELIVERY_URL, value: "http://delivery:8080"}
- name: CODEHUB_DELIVERY_URLS
  {{- $deliveryURLs := list }}
  {{- range $region := $orderedRegions }}
  {{- $deliveryURLs = append $deliveryURLs (printf "http://delivery.codehub-%s.svc.cluster.local:8080" $region) }}
  {{- end }}
  value: {{ join "," $deliveryURLs | quote }}
- {name: CODEHUB_WEBHOOK_HOSTS, value: {{ join "," .Values.webhookHosts | quote }}}
- {name: OTEL_EXPORTER_OTLP_ENDPOINT, value: {{ .Values.observability.otlpEndpoint | quote }}}
{{- end -}}
