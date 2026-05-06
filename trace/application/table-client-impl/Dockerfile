FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY . .
RUN mvn -B -f trace/application/table-client-impl/pom.xml clean package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app

COPY --from=build /workspace/trace/application/table-client-impl/target/trace-table-client-impl.jar /app/trace-table-client-impl.jar
COPY --from=build /workspace/trace/application/table-client-impl/target/libs /app/libs

ENTRYPOINT ["java", "-cp", "/app/trace-table-client-impl.jar:/app/libs/*", "tech.ydb.trace.table.TableClientImplTraceApp"]
