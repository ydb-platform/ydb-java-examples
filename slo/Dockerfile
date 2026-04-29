# Multi-stage Dockerfile for the YDB Java SDK SLO workload.
#
# The image is consumed by the YDB SLO action (`ydb-platform/ydb-slo-action`).
# Two instances run in parallel — current and baseline — under the same chaos
# conditions, and their metrics are compared by the report action.
#
# Build context: a directory that contains TWO checkouts side by side:
#   ./ydb-java-sdk         the SDK source under test (current or baseline)
#   ./ydb-java-examples    the SLO workload sources
#
# The CI workflow assembles this layout in a temp directory and passes it as
# the docker build context, so this Dockerfile can build the SDK from source
# and then the workload against that exact build — without ever needing the
# SDK to be published to a remote Maven repository.
#
# Optional build args:
#   MAVEN_IMAGE   Builder image. Defaults to `maven:3.9-eclipse-temurin-17`.
#   RUNTIME_IMAGE Runtime image. Defaults to `eclipse-temurin:17-jre`.

ARG MAVEN_IMAGE=maven:3.9-eclipse-temurin-17
ARG RUNTIME_IMAGE=eclipse-temurin:17-jre

# ---------- builder: install the SDK ---------------------------------------
FROM ${MAVEN_IMAGE} AS workload-build

WORKDIR /src

# Copy only the SDK checkout into the builder so changes elsewhere in the
# context don't invalidate this layer's cache.
COPY ydb-java-sdk /src/ydb-java-sdk

# Install the SDK (and its BOM) into the in-image local Maven repository at
# /root/.m2/repository. Tests are skipped — the SDK has its own CI for that;
# here we only need the artifacts. We also skip javadoc/source jars because
# the workload doesn't need them.
RUN cd /src/ydb-java-sdk && \
    mvn -B -q \
        -DskipTests \
        -Dmaven.javadoc.skip=true \
        -Dmaven.source.skip=true \
        -Dgpg.skip=true \
        install

# Capture the SDK version into a small file so the next stage can read it
# without parsing the pom again. `help:evaluate` is quiet enough to be safe in
# scripts.
RUN cd /src/ydb-java-sdk && \
    mvn -B -q help:evaluate -Dexpression=project.version -DforceStdout > /tmp/sdk.version && \
    echo "Built SDK version: $(cat /tmp/sdk.version)"

# Copy the examples checkout. We do this in a separate stage so changes to
# the workload code don't invalidate the SDK install layer above.
COPY ydb-java-examples /src/ydb-java-examples

# Override the SDK version pinned in the examples parent pom to point at the
# version we just installed. This lets us test SDK SNAPSHOTs without
# publishing anywhere.
RUN cd /src/ydb-java-examples && \
    SDK_VERSION="$(cat /tmp/sdk.version)" && \
    echo "Pinning ydb-java-examples to SDK ${SDK_VERSION}" && \
    mvn -B -q versions:set-property \
        -Dproperty=ydb.sdk.version \
        -DnewVersion="${SDK_VERSION}" \
        -DgenerateBackupPoms=false

# Build only the slo module (and its required parent/BOM context). The
# examples parent pom lists many modules; `-pl slo -am` keeps the build
# focused on what the workload actually needs.
RUN cd /src/ydb-java-examples && \
    mvn -B -q \
        -pl slo -am \
        -DskipTests \
        -Dmaven.javadoc.skip=true \
        package

# ---------- runtime --------------------------------------------------------
FROM ${RUNTIME_IMAGE}

WORKDIR /app

# Copy the executable jar plus its transitive dependencies. The slo pom is
# configured to drop dependencies into target/libs and to set the manifest
# Class-Path to libs/, so a single `java -jar` call is enough.
COPY --from=workload-build /src/ydb-java-examples/slo/target/ydb-slo-workload.jar /app/ydb-slo-workload.jar
COPY --from=workload-build /src/ydb-java-examples/slo/target/libs /app/libs

ENTRYPOINT ["java", "-jar", "/app/ydb-slo-workload.jar"]
