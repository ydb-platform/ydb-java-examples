# Multi-stage Dockerfile for the YDB JDBC SLO workload.
#
# The image can be consumed by the YDB SLO action
# (`ydb-platform/ydb-slo-action`): the workload reads its connection details
# and run parameters from environment variables and pushes OTLP metrics to the
# endpoint the action provides.
#
# Build context: the `ydb-java-examples` repository root.
#
# Optional build args:
#   MAVEN_IMAGE     Builder image. Defaults to `maven:3.9-eclipse-temurin-17`.
#   RUNTIME_IMAGE   Runtime image. Defaults to `eclipse-temurin:17-jre`.
#   YDB_JDBC_VERSION Override the ydb-jdbc-driver version under test.

ARG MAVEN_IMAGE=maven:3.9-eclipse-temurin-17
ARG RUNTIME_IMAGE=eclipse-temurin:17-jre

# ---------- builder ---------------------------------------------------------
FROM ${MAVEN_IMAGE} AS workload-build

WORKDIR /src
COPY . /src

ARG YDB_JDBC_VERSION=""

# Pin the JDBC driver version under test when provided, then build only the
# workload module (and the parent context it needs).
RUN if [ -n "${YDB_JDBC_VERSION}" ]; then \
        echo "Pinning ydb-jdbc-driver to ${YDB_JDBC_VERSION}" && \
        mvn -B -q versions:set-property \
            -Dproperty=ydb.jdbc.version \
            -DnewVersion="${YDB_JDBC_VERSION}" \
            -DgenerateBackupPoms=false \
            -pl slo-workload ; \
    fi && \
    mvn -B -q -pl slo-workload/jdbc -am \
        -DskipTests \
        -Dmaven.javadoc.skip=true \
        package

# ---------- runtime ---------------------------------------------------------
FROM ${RUNTIME_IMAGE}

WORKDIR /app

# The jar's manifest Class-Path points at libs/, so a single `java -jar` call
# is enough.
COPY --from=workload-build /src/slo-workload/jdbc/target/ydb-slo-jdbc-workload.jar /app/ydb-slo-jdbc-workload.jar
COPY --from=workload-build /src/slo-workload/jdbc/target/libs /app/libs

ENTRYPOINT ["java", "-jar", "/app/ydb-slo-jdbc-workload.jar"]
