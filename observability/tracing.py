"""
OpenTelemetry tracer setup.

Falls back gracefully to a no-op tracer when OTEL_EXPORTER_OTLP_ENDPOINT
is not set, so the code remains unchanged whether tracing is enabled or not.

The correlation_id from each resolve request is propagated as a span attribute
so a single trace connects: incoming_event → thinker → executor → HITL decision.
"""

import logging
import os
from contextlib import contextmanager
from typing import Generator

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter

logger = logging.getLogger(__name__)

_tracer: trace.Tracer | None = None


def setup_tracing(service_name: str = "synapse-agent") -> trace.Tracer:
    global _tracer

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)

    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    if otlp_endpoint:
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
            exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=True)
            provider.add_span_processor(BatchSpanProcessor(exporter))
            logger.info("OTel tracing → %s", otlp_endpoint)
        except ImportError:
            logger.warning("opentelemetry-exporter-otlp-proto-grpc not installed; tracing disabled")
    else:
        # Dev fallback: emit spans to stdout so they're visible without a collector
        if os.getenv("OTEL_CONSOLE_EXPORT", "").lower() == "true":
            provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

    trace.set_tracer_provider(provider)
    _tracer = trace.get_tracer(service_name)
    return _tracer


def get_tracer() -> trace.Tracer:
    global _tracer
    if _tracer is None:
        _tracer = setup_tracing()
    return _tracer


@contextmanager
def traced_span(
    name: str,
    correlation_id: str | None = None,
    attributes: dict | None = None,
) -> Generator[trace.Span, None, None]:
    """Context manager that wraps a block in an OTel span with correlation_id attached."""
    tracer = get_tracer()
    with tracer.start_as_current_span(name) as span:
        if correlation_id:
            span.set_attribute("synapse.correlation_id", correlation_id)
        for k, v in (attributes or {}).items():
            span.set_attribute(k, str(v))
        yield span
