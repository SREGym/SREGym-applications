"""Bounded request telemetry, without credentials or payload logging."""

import json
import logging
import os
import time
from collections import Counter
from threading import Lock

from fastapi import Request, Response


class Telemetry:
    def __init__(self, region: str):
        self.region = region
        self.counts = Counter()
        self.duration = Counter()
        self.lock = Lock()
        self.tracer = None
        self.latency = None
        self.requests = None
        if os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
            from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
            from opentelemetry.sdk.metrics import MeterProvider
            from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
            from opentelemetry.sdk.resources import Resource
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import BatchSpanProcessor

            resource = Resource.create(
                {
                    "service.name": os.environ.get("CODEHUB_SERVICE_NAME", "codehub"),
                    "cloud.region": region,
                    "service.instance.id": os.environ.get("HOSTNAME", "local"),
                }
            )
            provider = TracerProvider(resource=resource)
            provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
            self.tracer = provider.get_tracer("codehub.http")
            reader = PeriodicExportingMetricReader(OTLPMetricExporter(), export_interval_millis=10000)
            meter = MeterProvider(resource=resource, metric_readers=[reader]).get_meter("codehub.http")
            self.latency = meter.create_histogram("http.server.request.duration", unit="s")
            self.requests = meter.create_counter("http.server.requests", unit="{request}")

    async def middleware(self, request: Request, call_next):
        started = time.monotonic()
        status = 500
        span = None
        token = None
        if self.tracer:
            from opentelemetry import context, trace
            from opentelemetry.propagate import extract

            span = self.tracer.start_span(
                f"{request.method} HTTP", context=extract(dict(request.headers)), kind=trace.SpanKind.SERVER
            )
            token = context.attach(trace.set_span_in_context(span))
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            route = request.scope.get("route")
            label = route.path if route else "unmatched"
            elapsed = time.monotonic() - started
            attributes = {
                "http.request.method": request.method,
                "http.route": label,
                "http.response.status_code": status,
                "cloud.region": self.region,
            }
            if self.tracer:
                from opentelemetry import context

                span.update_name(f"{request.method} {label}")
                span.set_attributes(attributes)
                span.end()
                context.detach(token)
                self.latency.record(elapsed, attributes=attributes)
                self.requests.add(1, attributes=attributes)
            with self.lock:
                self.counts[(request.method, label, status)] += 1
                self.duration[(request.method, label)] += elapsed
            logging.getLogger("codehub.requests").info(
                json.dumps(
                    {
                        "region": self.region,
                        "method": request.method,
                        "route": label,
                        "status": status,
                        "duration_seconds": round(elapsed, 6),
                    }
                )
            )

    def metrics(self):
        lines = ["# TYPE codehub_requests_total counter", "# TYPE codehub_request_duration_seconds_sum counter"]
        with self.lock:
            for (method, route, status), count in sorted(self.counts.items()):
                labels = f'region="{self.region}",method="{method}",route="{route}",status="{status}"'
                lines.append(f"codehub_requests_total{{{labels}}} {count}")
            for (method, route), elapsed in sorted(self.duration.items()):
                labels = f'region="{self.region}",method="{method}",route="{route}"'
                lines.append(f"codehub_request_duration_seconds_sum{{{labels}}} {elapsed}")
        return Response("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")
