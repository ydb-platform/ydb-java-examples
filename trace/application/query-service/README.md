# QueryService trace demo

This demo is a standalone Java application with programmatic OpenTelemetry setup.
It configures OTLP exporter in code and adds `GrpcTelemetry` interceptor to the YDB gRPC channel builder.

- Source style: `query-example` scenario (implemented locally in `trace/`)
- Main class: `tech.ydb.trace.query.QueryServiceTraceApp`
- Service name in traces: `ydb-query-service-example`
