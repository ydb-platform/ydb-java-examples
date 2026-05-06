FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /workspace

COPY . .
RUN mvn -B -f trace/application/query-service/pom.xml clean package -DskipTests

FROM eclipse-temurin:21-jre
WORKDIR /app

COPY --from=build /workspace/trace/application/query-service/target/trace-query-service.jar /app/trace-query-service.jar
COPY --from=build /workspace/trace/application/query-service/target/libs /app/libs

ENTRYPOINT ["java", "-cp", "/app/trace-query-service.jar:/app/libs/*", "tech.ydb.trace.query.QueryServiceTraceApp"]
