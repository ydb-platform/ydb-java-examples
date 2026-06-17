# Multi-stage Dockerfile for the Spring Data JPA / Hibernate 6 SLO workload.
#
# Build context: the `ydb-java-examples` repository root. For ydb-jdbc-driver
# CI the context may also contain `./ydb-jdbc-driver`; when present, the driver
# is installed from source and the workload is pinned to that exact version.
#
# Optional build args:
#   MAVEN_IMAGE       Builder image. Defaults to `maven:3.9-eclipse-temurin-17`.
#   RUNTIME_IMAGE     Runtime image. Defaults to `eclipse-temurin:17-jre`.
#   YDB_JDBC_VERSION  Override the ydb-jdbc-driver version under test.

ARG MAVEN_IMAGE=maven:3.9-eclipse-temurin-17
ARG RUNTIME_IMAGE=eclipse-temurin:17-jre

FROM ${MAVEN_IMAGE} AS workload-build

WORKDIR /src
COPY . /src

ARG YDB_JDBC_VERSION=""

RUN if [ -d /src/ydb-jdbc-driver ]; then \
        cd /src/ydb-jdbc-driver && \
        mvn -B -q \
            -DskipTests \
            -Dmaven.javadoc.skip=true \
            -Dmaven.source.skip=true \
            -Dgpg.skip=true \
            install && \
        mvn -B -q help:evaluate -Dexpression=project.version -DforceStdout > /tmp/ydb-jdbc.version && \
        YDB_JDBC_VERSION="$(cat /tmp/ydb-jdbc.version)" && \
        cd /src && \
        echo "Pinning ydb-jdbc-driver to source-built ${YDB_JDBC_VERSION}" && \
        mvn -B -q versions:set-property \
            -Dproperty=ydb.jdbc.version \
            -DnewVersion="${YDB_JDBC_VERSION}" \
            -DgenerateBackupPoms=false \
            -pl slo-workload ; \
    elif [ -n "${YDB_JDBC_VERSION}" ]; then \
        echo "Pinning ydb-jdbc-driver to ${YDB_JDBC_VERSION}" && \
        mvn -B -q versions:set-property \
            -Dproperty=ydb.jdbc.version \
            -DnewVersion="${YDB_JDBC_VERSION}" \
            -DgenerateBackupPoms=false \
            -pl slo-workload ; \
    fi && \
    mvn -B -q -pl slo-workload/spring-data-jpa -am \
        -DskipTests \
        -Dmaven.javadoc.skip=true \
        package

FROM ${RUNTIME_IMAGE}

WORKDIR /app
COPY --from=workload-build /src/slo-workload/spring-data-jpa/target/ydb-slo-spring-data-jpa-workload.jar /app/ydb-slo-spring-data-jpa-workload.jar

ENTRYPOINT ["java", "-jar", "/app/ydb-slo-spring-data-jpa-workload.jar"]
