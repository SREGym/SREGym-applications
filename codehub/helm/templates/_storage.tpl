{{- define "codehub.storage" -}}
{{- $root := .root -}}
{{- $role := .role -}}
{{- $service := index $root.Values.services $role -}}
apiVersion: apps/v1
kind: StatefulSet
metadata: {name: {{ $role }}}
spec:
  serviceName: {{ $role }}-headless
  replicas: {{ if eq $role "search" }}{{ $root.Values.replicas.search }}{{ else }}1{{ end }}
  podManagementPolicy: Parallel
  selector: {matchLabels: {app.kubernetes.io/name: codehub, app.kubernetes.io/component: {{ $role }}}}
  template:
    metadata:
      labels:
        {{- include "codehub.labels" $root | nindent 8 }}
        app.kubernetes.io/component: {{ $role }}
    spec:
      automountServiceAccountToken: false
      securityContext:
        {{- include "codehub.security" $root | nindent 8 }}
      {{- include "codehub.placement" (dict "root" $root "component" $role) | nindent 6 }}
      containers:
        - name: {{ $role }}
          image: {{ $root.Values.image.repository | quote }}
          imagePullPolicy: {{ $root.Values.image.pullPolicy }}
          command: [python, -m, codehub.{{ $role }}]
          securityContext: {allowPrivilegeEscalation: false, readOnlyRootFilesystem: true, capabilities: {drop: [ALL]}}
          env:
            {{- include "codehub.env" $root | nindent 12 }}
          ports: [{name: http, containerPort: 8080}]
          startupProbe: {httpGet: {path: /healthz, port: http}, periodSeconds: 2, failureThreshold: 90}
          readinessProbe:
            httpGet: {path: /readyz, port: http}
            periodSeconds: 3
            {{- if eq $role "repository" }}
            timeoutSeconds: 5
            {{- end }}
          livenessProbe: {httpGet: {path: /healthz, port: http}, periodSeconds: 10}
          resources:
            {{- toYaml $service.resources | nindent 12 }}
          volumeMounts:
            - {name: credentials, mountPath: /run/credentials, readOnly: true}
            - {name: data, mountPath: /var/lib/codehub}
            - {name: tmp, mountPath: /tmp}
            - {name: routes, mountPath: /etc/codehub, readOnly: true}
            {{- if eq $role "delivery" }}
            - {name: artifacts, mountPath: /var/lib/codehub/artifacts}
            {{- end }}
      volumes:
        - {name: credentials, secret: {secretName: {{ $root.Values.credentialsSecret }}, defaultMode: 0440}}
        - {name: tmp, emptyDir: {sizeLimit: 128Mi}}
        - {name: routes, configMap: {name: database-routes}}
  volumeClaimTemplates:
    - metadata: {name: data}
      spec:
        accessModes: [ReadWriteOnce]
        storageClassName: {{ $root.Values.storageClass | quote }}
        resources: {requests: {storage: {{ $service.storage | quote }}}}
    {{- if eq $role "delivery" }}
    - metadata: {name: artifacts}
      spec:
        accessModes: [ReadWriteOnce]
        storageClassName: {{ $root.Values.storageClass | quote }}
        resources: {requests: {storage: {{ $service.artifactStorage | quote }}}}
    {{- end }}
---
apiVersion: v1
kind: Service
metadata: {name: {{ $role }}}
spec:
  selector: {app.kubernetes.io/name: codehub, app.kubernetes.io/component: {{ $role }}}
  ports: [{name: http, port: 8080, targetPort: http}]
---
apiVersion: v1
kind: Service
metadata: {name: {{ $role }}-headless}
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector: {app.kubernetes.io/name: codehub, app.kubernetes.io/component: {{ $role }}}
  ports: [{name: http, port: 8080, targetPort: http}]
{{- end -}}
