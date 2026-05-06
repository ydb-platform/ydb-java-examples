# TableClientImpl trace demo

This demo is a standalone Java application with programmatic OpenTelemetry setup.
It configures OTLP exporter in code and adds `GrpcTelemetry` interceptor to the YDB gRPC channel builder.

- Source style: `basic_example` scenario (implemented locally in `trace/`)
- Main class: `tech.ydb.trace.table.TableClientImplTraceApp`
- Service name in traces: `ydb-table-client-impl-example`
